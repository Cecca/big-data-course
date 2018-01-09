package it.unipd.dei.bd1718;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.random.RandomRDDs;
import org.apache.spark.mllib.rdd.RandomRDD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * Utilities to generate random datasets
 */
public class Generate {

  private static class Args {

    @Parameter(names = "--output-prefix", required = true, description = "Prefix for the output name")
    String output;

    @Parameter(names = "--dimensions", description = "Number of dimensions for vectors")
    int dims = 3;

    @Parameter(names = "--num-points", description = "Number of points")
    int numPoints = 1000;

    @Parameter(names = "--num-clusters", description = "Number of clusters")
    int numClusters = 1;

  }

  public static void main(String[] args) throws IOException {

    Args arguments = new Args();
    JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

    SparkConf conf = new SparkConf(true)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .setAppName("Dataset generation");
    JavaSparkContext sc = new JavaSparkContext(conf);
    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

    String outfile = arguments.output + "-" + arguments.dims + "-" + arguments.numPoints + "-" + arguments.numClusters;

    if (fs.exists(new Path(outfile))) {
      throw new RuntimeException("Output path `" + outfile + "` already exists");
    }

    Random rnd = new Random();
    Vector[] centers = new Vector[arguments.numClusters];
    for (int i=0; i<arguments.numClusters; i++) {
      double[] data = new double[arguments.dims];
      for (int j=0; j<arguments.dims; i++) {
        data[j] = rnd.nextDouble();
      }
      centers[i] = new DenseVector(data);
    }

    int pointsPerCluster = arguments.numPoints / arguments.numClusters;

    JavaRDD<Vector> vectors = null;
    for (Vector c : centers) {
      JavaRDD<Vector> randomVecs = RandomRDDs.normalJavaVectorRDD(sc, pointsPerCluster, arguments.dims)
              .map((v) -> LinAlgOps.sum(v, c));
      if (vectors == null) {
        vectors = randomVecs;
      } else {
        vectors = vectors.union(randomVecs);
      }
    }

    // Save vectors
    InputOutput.writeVectors(vectors, outfile);
  }

}
