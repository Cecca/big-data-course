package it.unipd.dei.bd1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SecondHomework {

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException("This program accepts just the path to the input file");
    }

    String path = args[0];

    // Setup Spark
    SparkConf conf = new SparkConf(true)
      .setMaster("local")
      .setAppName("WordCount");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> words = sc.textFile(path, 16).cache();
    words.count();

    long start = System.currentTimeMillis();

    // Your code here

    long end = System.currentTimeMillis();
    System.out.println("Elapsed time: " + (end - start) + " ms");
    System.out.println("Press enter to finish");
    System.in.read();
  }

}
