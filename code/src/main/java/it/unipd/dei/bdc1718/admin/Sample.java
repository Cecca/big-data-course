package it.unipd.dei.bdc1718.admin;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import it.unipd.dei.bdc1718.InputOutput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Sample {

  private static Logger logger = LoggerFactory.getLogger(Sample.class);

  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "--basename", required = true, description = "Path to the output vector dataset")
    String output;

    @Parameter(names = "--size", required = true, description = "The size of the output")
    long size;

    @Parameter(names = "--coalesce", description = "Whether to coalesce into a single partition")
    boolean coalesce = false;

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

    JavaRDD<Vector> vecs = InputOutput.readVectors(sc, arguments.input).cache();
    long cnt = vecs.count();
    logger.info("The count of vectors is {}", cnt);
    int dims = vecs.take(1).get(0).size();
    String output = arguments.output + "-" + dims + "-" + arguments.size;
    double prob = arguments.size / ((double) cnt);
    logger.info("Sampling with probability {}", prob);
    JavaRDD<Vector> sample = vecs.sample(false, prob).cache();
    long sampleCnt = sample.count();
    logger.info("Sampled {} vectors", sampleCnt);
    if (arguments.coalesce) {
      Output.writeVectorsSeq(sample, output);
    } else {
      Output.writeVectors(sample, output);
    }
  }

}
