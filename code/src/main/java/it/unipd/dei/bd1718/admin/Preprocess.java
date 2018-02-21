package it.unipd.dei.bd1718.admin;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.SizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Preprocess {

  private static Logger logger = LoggerFactory.getLogger(Preprocess.class);

  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "--output", required = true, description = "Path to the output vector dataset")
    String output;

    @Parameter(names = "--lemmas", required = true, description = "Lemmatized input, to speed up execution")
    String lemmas;

    @Parameter(names = "--dimensions", description = "Number of dimensions for word2vec vectors")
    int dims = 100;

    @Parameter(names = "--iterations", description = "Number of iterations for word2vec training")
    int iterations = 10;

    @Parameter(names = "--min-count", description = "Minimum number of occurrences of each word in word2vec training")
    int minCount = 3;

    @Parameter(names = "--partitions", description = "Partitions to use for word2vec training")
    int partitions = 1;

    @Parameter(names = "--model", description = "Path to the word2vec model. If not existing, a new one will be trained using the dataset as input")
    String model;

    @Parameter(names = "--glove", description = "Path to the glove model.")
    String glove;

  }

  private static void writeSentences(JavaPairRDD<Long, ArrayList<ArrayList<String>>> docSentences, String path) {
    docSentences.mapPartitionsToPair((it) -> {
      Kryo kryo = new Kryo();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output out = new Output(bos);
      while(it.hasNext()) {
        Tuple2<Long, ArrayList<ArrayList<String>>> p = it.next();
        out.writeLong(p._1());
        kryo.writeClassAndObject(out, p._2());
      }
      out.close();
      BytesWritable bytes = new BytesWritable(bos.toByteArray());
      return Collections.singleton(new Tuple2<>(NullWritable.get(), bytes)).iterator();
    }).saveAsNewAPIHadoopFile(path, NullWritable.class, BytesWritable.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
  }

  private static JavaPairRDD<Long, ArrayList<ArrayList<String>>> readSentences(JavaSparkContext sc, String path) {
     return sc.newAPIHadoopFile(path, org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class, NullWritable.class, BytesWritable.class, sc.hadoopConfiguration())
             .values()
             .flatMapToPair((bytes) -> {
               Kryo kryo = new Kryo();
               Input input = new Input(((BytesWritable) bytes).getBytes());
               ArrayList<Tuple2<Long, ArrayList<ArrayList<String>>>> tuples = new ArrayList<>();
               while(!input.eof()) {
                 Tuple2<Long, ArrayList<ArrayList<String>>> t = new Tuple2<>(
                         input.readLong(),
                         (ArrayList<ArrayList<String>>) kryo.readClassAndObject(input)
                 );
                 if (t._2() == null) {
                   break;
                 }
                 tuples.add(t);
               }
               return tuples.iterator();
            });
  }

  private static JavaPairRDD<Long, ArrayList<ArrayList<String>>> loadLemmas(JavaSparkContext sc, Args arguments) throws IOException {
    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

    if (fs.exists(new Path(arguments.lemmas))) {
      logger.info("Lemmas file exists");
      return readSentences(sc, arguments.lemmas);
    } else {
      logger.info("Lemmas file does not exist, creating");
      JavaRDD<WikiPage> pages = InputOutput.read(sc, arguments.input)
              .filter((wp) -> !wp.getTitle().contains("disambiguation"));

      JavaPairRDD<Long, ArrayList<ArrayList<String>>> docSentences = pages.mapToPair((wp) -> {
        ArrayList<ArrayList<String>> sents = Lemmatizer.lemmatizedSentences(wp.getText());
        return new Tuple2<>(wp.getId(), sents);
      });

      writeSentences(docSentences, arguments.lemmas);

      return readSentences(sc, arguments.lemmas);
    }
  }

  private static Word2VecModel trainWord2Vec(JavaRDD<ArrayList<String>> sentences, Args arguments) {
    return new Word2Vec()
            .setVectorSize(arguments.dims)
            .setMinCount(arguments.minCount)
            .setNumIterations(arguments.iterations)
            .setNumPartitions(arguments.partitions)
            .fit(sentences);
  }

  private static Map<String, Vector> loadModel(
          JavaPairRDD<Long, ArrayList<ArrayList<String>>> docSentences,
          FileSystem fs,
          Args arguments) throws IOException {
    SparkContext sc = docSentences.context();
    if (arguments.glove != null) {
      Map<String, Vector> result = new HashMap<>();

      List<Tuple2<String, Vector>> vecs =
              JavaSparkContext.fromSparkContext(sc).textFile(arguments.glove).mapToPair((line) -> {
                String[] tokens = line.split(" ");
                double[] values = new double[tokens.length - 1];
                for (int i = 0; i< values.length; i++) {
                  values[i] = Double.parseDouble(tokens[i+1]);
                }
                Vector v = Vectors.dense(values);
                return new Tuple2<>(tokens[0], v);
              }).collect();
      for (Tuple2<String, Vector> t : vecs) {
        if (t._2() == null) {
          throw new RuntimeException("Null vector for word " + t._1());
        }
        result.put(t._1(), t._2());
      }
      return result;
    } else {
      if (arguments.model == null) {
        throw new IllegalArgumentException("You should provide either --model or --glove");
      }
      Word2VecModel w2v;
      if (fs.exists(new Path(arguments.model))) {
        w2v = Word2VecModel.load(sc, arguments.model);
      } else {
        w2v = trainWord2Vec(
                docSentences.values().flatMap((sents) -> sents.iterator()),
                arguments);
        w2v.save(sc, arguments.model);
      }
      Map<String, Vector> result = new HashMap<>();
      scala.collection.Iterator<String> it = w2v.getVectors().keysIterator();
      while (it.hasNext()) {
        String word = it.next();
        result.put(word, w2v.transform(word));
      }
      return result;
    }
  }

  public static void main(String[] args) throws IOException {
    System.out.println("Reading command line options");
    Args arguments = new Args();
    JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

    SparkConf conf = new SparkConf(true)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .setAppName("Preprocess");
    JavaSparkContext sc = new JavaSparkContext(conf);
    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

    // Verify command line arguments
    if (fs.exists(new Path(arguments.output))) {
      logger.error("output already exists, not overwriting");
      return;
    }

    Broadcast<Set<String>> bStopWords = broadcastStopwords(sc);

    JavaPairRDD<Long, ArrayList<ArrayList<String>>> docSentences =
            loadLemmas(sc, arguments);

    Map<String, Vector> model = loadModel(docSentences, fs, arguments);
    final int dimensions = model.get("be").size();
    logger.info("Loaded model with {} entries in {} dimensions (estimated {} Mb)",
            model.size(), dimensions, SizeEstimator.estimate(model) / 1024.0);

    LongAccumulator skippedPages = sc.sc().longAccumulator("skipped-pages");
    LongAccumulator skippedLemmas = sc.sc().longAccumulator("skipped-lemmas");
    Broadcast<Map<String, Vector>> bModel = sc.broadcast(model);
    JavaPairRDD<Long, Vector> vectors = docSentences
            .flatMapToPair((pair) -> {
              Map<String, Vector> lModel = bModel.getValue();
              double vec[] = new double[dimensions];
              AtomicInteger counter = new AtomicInteger();
              AtomicInteger skipped = new AtomicInteger();
              for (ArrayList<String> sentence : pair._2()) {
                for (String l : sentence) {
                  String lemma = l.toLowerCase();
                  if (!bStopWords.getValue().contains(lemma)) {
                    if (lModel.containsKey(lemma)) {
                      double[] lemVec = lModel.get(lemma).toArray();
                      for (int i = 0; i < dimensions; i++) {
                        vec[i] += lemVec[i];
                      }
                      counter.incrementAndGet();
                    } else {
                      skipped.incrementAndGet();
                      logger.warn("Skipping `" + lemma + "` since it's missing from the vocabulary");
                    }
                  }
                }
              }
              skippedLemmas.add(skipped.intValue());
              int numLemmas = counter.intValue();
              if (numLemmas == 0) {
                skippedPages.add(1);
                logger.warn("Skipping id " + pair._1() + " because no lemmas were mapped");
                return Collections.emptyIterator();
              }
              for (int i=0; i<dimensions; i++) {
                vec[i] /= numLemmas;
              }

              Vector result = Vectors.dense(vec);
              return Collections.singleton(new Tuple2<>(pair._1(), result)).iterator();
            }).cache();

    InputOutput.writeVectors(vectors.values(), arguments.output);

    logger.info("Output written. {} pages skipped. {} lemmas skipped (overall)", skippedPages.value(), skippedLemmas.value());
  }

  private static Broadcast<Set<String>> broadcastStopwords(JavaSparkContext sc) {
    String[] lStopWordsArr = StopWordsRemover.loadDefaultStopWords("english");
    HashSet<String> lStopWords = new HashSet<>();
    for (String w : lStopWordsArr) {
      lStopWords.add(w);
    }
    return sc.broadcast(lStopWords);
  }

}
