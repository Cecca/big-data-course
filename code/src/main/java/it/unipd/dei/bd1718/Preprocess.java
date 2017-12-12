package it.unipd.dei.bd1718;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
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
import org.apache.spark.rdd.SequenceFileRDDFunctions;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

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

    @Parameter(names = "--model", required = true, description = "Path to the word2vec model. If not existing, a new one will be trained using the dataset as input")
    String model;

  }

  private static JavaPairRDD<Long, ArrayList<ArrayList<String>>> loadLemmas(JavaSparkContext sc, Args arguments) throws IOException {
    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

    if (fs.exists(new Path(arguments.lemmas))) {
      logger.info("Lemmas file exists");
      return sc.objectFile(arguments.lemmas)
              .mapToPair((p) -> (Tuple2<Long, ArrayList<ArrayList<String>>>) p);
    } else {
      logger.info("Lemmas file does not exist, creating");
      JavaRDD<WikiPage> pages = InputOutput.read(sc, arguments.input)
              .filter((wp) -> !wp.getTitle().contains("disambiguation"));

      JavaPairRDD<Long, ArrayList<ArrayList<String>>> docSentences = pages.mapToPair((wp) -> {
        ArrayList<ArrayList<String>> sents = Lemmatizer.lemmatizedSentences(wp.getText());
        return new Tuple2<>(wp.getId(), sents);
      });

      docSentences.saveAsObjectFile(arguments.lemmas);
      return sc.objectFile(arguments.lemmas)
              .mapToPair((p) -> (Tuple2<Long, ArrayList<ArrayList<String>>>) p);
    }
  }

  private static Word2VecModel trainWord2Vec(JavaRDD<ArrayList<String>> sentences, Args arguments) {
    return new Word2Vec()
            .setVectorSize(arguments.dims)
            .setMinCount(arguments.minCount)
            .setNumIterations(arguments.iterations)
            .fit(sentences);
  }

  public static void main(String[] args) throws IOException {
    System.out.println("Reading command line options");
    Args arguments = new Args();
    JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

    final int dimensions = arguments.dims;

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

    Word2VecModel w2v;
    if (fs.exists(new Path(arguments.model))) {
      w2v = Word2VecModel.load(sc.sc(), arguments.model);
    } else {
      w2v = trainWord2Vec(
              docSentences.values().flatMap((sents) -> sents.iterator()),
              arguments);
      w2v.save(sc.sc(), arguments.model);
    }

    Broadcast<Word2VecModel> bw2v = sc.broadcast(w2v);
    JavaPairRDD<Long, Vector> vectors = docSentences
            .mapToPair((pair) -> {
              double vec[] = new double[dimensions];
              AtomicInteger counter = new AtomicInteger();
              for (ArrayList<String> sentence : pair._2()) {
                for (String lemma : sentence) {
                  try {
                    if (!bStopWords.getValue().contains(lemma)) {
                      double[] lemVec = bw2v.getValue().transform(lemma).toArray();
                      for (int i = 0; i < dimensions; i++) {
                        vec[i] += lemVec[i];
                      }
                      counter.incrementAndGet();
                    }
                  } catch (IllegalStateException e) {
                    System.err.println("WARNING: Skipping `" + lemma + "` since it's missing from the vocabulary");
                  }
                }
              }
              int numLemmas = counter.intValue();
              for (int i=0; i<dimensions; i++) {
                vec[i] /= numLemmas;
              }

              Vector result = Vectors.dense(vec);
              return new Tuple2<>(pair._1(), result);
            }).cache();

    InputOutput.writeVectors(vectors, arguments.output);

    JavaPairRDD<Long, Vector> vectorsCheck =
            InputOutput.readVectors(sc, arguments.output);
    JavaPairRDD<Long, List<Tuple2<String, Object>>> synonyms =
            vectorsCheck.mapValues((v) -> Arrays.asList(bw2v.getValue().findSynonyms(v, 3)));

    JavaRDD<WikiPage> pages = InputOutput.read(sc, arguments.input)
            .filter((wp) -> !wp.getTitle().contains("disambiguation"));
    pages.mapToPair((wp) -> new Tuple2<>(wp.getId(), wp))
            .join(synonyms)
            .takeSample(false, 10)
            .stream()
            .forEach((tup) -> {
              System.out.println(
                      "Page `" + tup._2()._1().getTitle() +
                      "` with closest vectors " + tup._2()._2());
            });
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
