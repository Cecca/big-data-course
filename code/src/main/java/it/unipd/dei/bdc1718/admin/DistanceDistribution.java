package it.unipd.dei.bdc1718.admin;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import it.unipd.dei.bdc1718.InputOutput;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class DistanceDistribution {

  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "--output", required = true, description = "Path to the output vector dataset")
    String output;

    @Parameter(names = "--sample", required = true)
    int sampleSize;

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

    JavaRDD<Vector> vectors =InputOutput.readVectors(sc, arguments.input).cache();
    long cnt = vectors.count();
    JavaRDD<Vector> sample = vectors
      .sample(false, arguments.sampleSize / ((double) cnt));
    List<Double> distances =
      sample.cartesian(sample).map((t) -> Vectors.sqdist(t._1(), t._2())).collect();
    FileWriter fw = new FileWriter(arguments.output);
    for (double d : distances) {
      fw.write(d + "\n");
    }
    fw.close();
  }
}
