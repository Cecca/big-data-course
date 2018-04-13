package it.unipd.dei.bdc1718;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.OutputWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Collection of utility input/output methods
 */
public class InputOutput {

  public static Vector strToVector(String str) {
    String[] tokens = str.split(" ");
    double[] data = new double[tokens.length];
    for (int i=0; i<tokens.length; i++) {
      data[i] = Double.parseDouble(tokens[i]);
    }
    return Vectors.dense(data);
  }

  public static String vectorToStr(Vector v) {
    StringBuffer sb = new StringBuffer();
    double[] data = v.toArray();
    for (int i=0; i<data.length-1; i++) {
      sb.append(data[i]).append(' ');
    }
    sb.append(data[data.length-1]);
    return sb.toString();
  }

  public static JavaRDD<Vector> readVectors(JavaSparkContext sc, String path) {
    return sc.textFile(path).map(InputOutput::strToVector);
  }

  public static ArrayList<Vector> readVectorsSeq(String path) throws IOException {
    if (Files.isDirectory(Paths.get(path))) {
      throw new IllegalArgumentException("readVectorsSeq is meant to read a single file. To read this path (which is a directory) use the spark-based `readVectors`");
    }

    ArrayList<Vector> result = new ArrayList<>();
    Files.lines(Paths.get(path))
            .map(InputOutput::strToVector)
            .forEach(result::add);
    return result;
  }

  public static void writeVectors(JavaRDD<Vector> vectors, String path) {
    vectors.map(InputOutput::vectorToStr)
            .saveAsTextFile(path, BZip2Codec.class);
  }

  public static void writeVectorsSeq(JavaRDD<Vector> vectors, String path) {
    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(path))) {
      PrintWriter pw = new PrintWriter(os);
      vectors.map(InputOutput::vectorToStr)
              .toLocalIterator()
              .forEachRemaining(pw::println);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

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
      writeVectors(readVectorsBin(sc, input), output);
    } else if ("txt2bin".equals(subcmd)) {
      writeVectorsBin(readVectors(sc, input), output);
    } else {
      throw new IllegalArgumentException("Unknown subcommand");
    }

  }

}
