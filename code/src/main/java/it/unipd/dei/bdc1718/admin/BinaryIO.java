package it.unipd.dei.bdc1718.admin;

import it.unipd.dei.bdc1718.InputOutput;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;

public class BinaryIO {

  public static JavaRDD<Vector> readVectorsBin(JavaSparkContext sc, String path) {
    return new SparkSession(sc.sc())
            .read()
            .parquet(path)
            .as(Encoders.kryo(Vector.class))
            .javaRDD();
  }

  public static void writeVectorsBin(JavaRDD<Vector> vectors, String path) {
    new SparkSession(vectors.context())
            .createDataset(vectors.rdd(), Encoders.kryo(Vector.class))
            .write()
            .option("compression", "gzip")
            .parquet(path);
  }

  public static ArrayList<Vector> readVectorsSequentialBin(String path) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("parquet reading");
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.setLogLevel("ERROR");
    ArrayList<Vector> data = new ArrayList<>();
    data.addAll(readVectorsBin(sc, path).collect());
    return data;
  }

  public static void main(String[] args) throws IOException {
    String subcmd = args[0];
    String input = args[1];
    String output = args[2];

    SparkConf conf = new SparkConf(true).setAppName("conversions");
    JavaSparkContext sc = new JavaSparkContext(conf);

    if ("bin2txt".equals(subcmd)) {
      Output.writeVectors(readVectorsBin(sc, input), output);
    } else if ("txt2bin".equals(subcmd)) {
      writeVectorsBin(InputOutput.readVectors(sc, input), output);
    } else {
      throw new IllegalArgumentException("Unknown subcommand");
    }

  }
}
