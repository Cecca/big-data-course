package it.unipd.dei.bd1718.diversity;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;

/**
 * Utility to get a sample of the given expected size.
 */
public class Sample {

  private static Logger logger = LoggerFactory.getLogger(Sample.class);

  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "--output", required = true, description = "Path to the output vector dataset")
    String output;

    @Parameter(names = "--size", required = true, description = "The (expected) size of the sample")
    int size;

  }

  public static void main(String[] args) throws IOException {
    Args arguments = new Args();
    JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

    SparkConf conf = new SparkConf(true)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .setAppName("Sample");
    JavaSparkContext sc = new JavaSparkContext(conf);
    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

    if (fs.exists(new Path(arguments.output))) {
      logger.error("output already exists, not overwriting");
      System.exit(1);
    }

    JavaRDD<Tuple2<Long, Vector>> vecs = InputOutput.readVectorsPairs(sc, arguments.input);
    JavaRDD<Tuple2<Long, Vector>> sample = vecs.sample(false, ((double) arguments.size) / vecs.count());
    InputOutput.writeVectorsPairs(JavaPairRDD.fromJavaRDD(sample), arguments.output);

  }

}
