package it.unipd.dei.bd1718.admin;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Collection of functions that allow to transform texts to sequence
 * of lemmas using lemmatization. An alternative process is
 * stemming. For a discussion of the difference between stemming and
 * lemmatization see this link: https://nlp.stanford.edu/IR-book/html/htmledition/stemming-and-lemmatization-1.html
 */
public class Lemmatizer {

  private static StanfordCoreNLP pipeline;

  private static Logger logger = LoggerFactory.getLogger(Lemmatizer.class);

  static {
    // Create StanfordCoreNLP object properties, with POS tagging
    // (required for lemmatization), and lemmatization
    Properties props;
    props = new Properties();
    props.put("annotators", "tokenize,ssplit,pos,lemma");

    // StanfordCoreNLP loads a lot of models, so you probably
    // only want to do this once per execution
    pipeline = new StanfordCoreNLP(props);
  }

  public static String lemmatize(String doc) {
    long start = System.currentTimeMillis();
    Annotation document = new Annotation(doc);
    pipeline.annotate(document);

    StringBuffer result = new StringBuffer();

    long numLemmas = 0;
    for (CoreLabel token : document.get(CoreAnnotations.TokensAnnotation.class)) {
      String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
      numLemmas++;
      result.append(lemma);
      result.append(' ');
    }
    long end = System.currentTimeMillis();
    logger.info("Performed initial annotation in " + (end - start) + " ms");
    double throughput = 1000 * ((double) numLemmas) / (end - start);
    logger.info("Annotation throughput " + throughput + " lemmas/s");

    return result.toString();
  }

  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "--output", required = true, description = "Path to the output vector dataset")
    String output;

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

    WikiPage.readPages(sc, arguments.input)
            .map((page) -> lemmatize(page.getText()))
            .saveAsTextFile(arguments.output);
  }

}