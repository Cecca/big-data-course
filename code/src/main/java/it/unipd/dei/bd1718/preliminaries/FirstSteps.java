package it.unipd.dei.bd1718.preliminaries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class FirstSteps {

  public static void main(String[] args) {
    if (args.length == 0) {
      throw new IllegalArgumentException("Expecting at least one number on the command line");
    }

    // Read a list of numbers from the program options
    ArrayList<Double> aNumbers = new ArrayList<>();
    for (String s : args) {
      aNumbers.add(Double.parseDouble(s));
    }

    // Setup Spark
    SparkConf conf = new SparkConf(true)
      .setAppName("Preliminaries");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a parallel collection
    JavaRDD<Double> dNumbers = sc.parallelize(aNumbers);

    // Apply a transform
    JavaRDD<Double> dSquares = dNumbers.map((x) -> x*x);

    // Get a sequential collection out of the parallel one.
    // Be careful with this operation: with large collections
    // it will crash your program, since it will use all the
    // memory available
    List<Double> lSquares = dSquares.collect();

    // Print out the squares
    for (double x : lSquares) {
      System.out.println(x);
    }
  }

}
