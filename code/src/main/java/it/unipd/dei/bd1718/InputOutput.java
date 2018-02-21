package it.unipd.dei.bd1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Collection of utility input/output methods
 */
public class InputOutput {

  public static JavaRDD<Vector> readVectors(JavaSparkContext sc, String path) {
    return new SparkSession(sc.sc())
            .read()
            .parquet(path)
            .as(Encoders.kryo(Vector.class))
            .javaRDD();
  }

  public static void writeVectors(JavaRDD<Vector> vectors, String path) {
    new SparkSession(vectors.context())
            .createDataset(vectors.rdd(), Encoders.kryo(Vector.class))
            .write()
            .option("compression", "gzip")
            .parquet(path);
  }

  public static ArrayList<Vector> readVectorsSequential(String path) throws IOException {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("parquet reading");
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.setLogLevel("ERROR");
    ArrayList<Vector> data = new ArrayList<>();
    data.addAll(readVectors(sc, path).collect());
    return data;
  }

}
