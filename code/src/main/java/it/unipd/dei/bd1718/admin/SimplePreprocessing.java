package it.unipd.dei.bd1718.admin;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class SimplePreprocessing {

  private static Logger logger = LoggerFactory.getLogger(SimplePreprocessing.class);

  private static StanfordCoreNLP pipeline;
  static {
    // Create StanfordCoreNLP object properties, just with tokenization
    Properties props;
    props = new Properties();
    props.put("annotators", "tokenize");

    // StanfordCoreNLP loads a lot of models, so you probably
    // only want to do this once per execution
    pipeline = new StanfordCoreNLP(props);
  }

  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "--output", required = true, description = "Path to the output vector dataset")
    String output;

    @Parameter(names = "--model", required = true, description = "Path to the glove model")
    String model;

  }

  private static Map<String, double[]> loadModel(JavaSparkContext sc, String path){
    Map<String, double[]> result = new HashMap<>();

    List<Tuple2<String, double[]>> vecs =
            sc.textFile(path).mapToPair((line) -> {
              String[] tokens = line.split(" ");
              double[] values = new double[tokens.length - 1];
              for (int i = 0; i< values.length; i++) {
                values[i] = Double.parseDouble(tokens[i+1]);
              }
              return new Tuple2<>(tokens[0], values);
            }).collect();
    for (Tuple2<String, double[]> t : vecs) {
      if (t._2() == null) {
        throw new RuntimeException("Null vector for word " + t._1());
      }
      result.put(t._1(), t._2());
    }
    return result;
  }

  private static JavaRDD<WikiPage> readPages(JavaSparkContext sc, String path) {
    return new SparkSession(sc.sc())
            .read()
            .json(path)
            .as(WikiPage.getEncoder())
            .javaRDD();
  }

  public static void main(String[] args) throws IOException {
    Args arguments = new Args();
    JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

    SparkConf conf = new SparkConf(true)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .setAppName("Simple preprocessing");
    JavaSparkContext sc = new JavaSparkContext(conf);
    logger.info(sc.getConf().toDebugString());
    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

    // Verify command line arguments
    if (fs.exists(new Path(arguments.output))) {
      logger.error("output already exists, not overwriting");
      return;
    }

    JavaRDD<WikiPage> pages = readPages(sc, arguments.input);
    Broadcast<Map<String, double[]>> bModel = sc.broadcast(loadModel(sc, arguments.model));
    int dim = bModel.getValue().get("be").length;

    LongAccumulator skippedWords = sc.sc().longAccumulator("Skipped pages");
    LongAccumulator totalWords = sc.sc().longAccumulator("Skipped words");
    LongAccumulator skippedPages = sc.sc().longAccumulator("Skipped pages");
    LongAccumulator totalPages = sc.sc().longAccumulator("Total pages");

    JavaRDD<Vector> vectors = pages.flatMap((page) -> {
      totalPages.add(1);
      Annotation doc = new Annotation(page.getText());
      pipeline.annotate(doc);

      double[] pageVector = new double[dim];

      int wordCnt = 0;
      for (CoreLabel token : doc.get(CoreAnnotations.TokensAnnotation.class)) {
        String word = token.get(CoreAnnotations.TextAnnotation.class).toLowerCase();
        totalWords.add(1);
        if (bModel.getValue().containsKey(word)) {
          double[] wordVector = bModel.getValue().get(word);
          for (int i=0; i<dim; i++) {
            pageVector[i] += wordVector[i];
            wordCnt += 1;
          }
        } else {
          skippedWords.add(1);
          logger.warn("Skipping `" + word + "` since it's missing from the vocabulary");
        }
      }
      if (wordCnt == 0) {
        skippedPages.add(1);
        return Collections.emptyIterator();
      }
      for (int i=0; i<dim; i++) {
        pageVector[i] = pageVector[i] / wordCnt;
      }
      return Collections.singleton(Vectors.dense(pageVector)).iterator();
    });

    it.unipd.dei.bd1718.InputOutput.writeVectors(vectors, arguments.output);

    logger.info("Done");
    logger.info("Skipped {} words over {}", skippedWords.value(), totalWords.value());
    logger.info("Skipped {} pages over {}", skippedPages.value(), totalPages.value());
  }

}
