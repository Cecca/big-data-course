package it.unipd.dei.bd1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class FirstHomework {

  public static void main(String[] args) {
    if (args.length == 0) {
      throw new IllegalArgumentException("Expecting at least one number on the command line");
    }

    // Read a list of numbers from the program options
    ArrayList<Double> lNumbers = new ArrayList<>();
    for (String s : args) {
      lNumbers.add(Double.parseDouble(s));
    }

    // Setup Spark
    SparkConf conf = new SparkConf(true)
      .setAppName("Preliminaries");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a parallel collection
    JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);

    // ================= Your code here ===================


  }

}
