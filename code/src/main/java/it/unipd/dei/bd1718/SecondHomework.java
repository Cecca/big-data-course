package it.unipd.dei.bd1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class SecondHomework {

  public static void main(String[] args) {
    if (args.length != 1) {
      throw new IllegalArgumentException("This program accepts just the path to the input file");
    }

    String path = args[0];

    // Setup Spark
    SparkConf conf = new SparkConf(true)
      .setMaster("local")
      .setAppName("WordCount");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> words = sc.textFile(path).repartition(4).cache();
    words.count();

    long start = System.currentTimeMillis();

//    Map<String, Integer> count = words.flatMapToPair((d) -> {
//      String[] tokens = d.split(" ");
//      ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
//      for (String token : tokens) {
//        pairs.add(new Tuple2<>(token, 1L));
//      }
//      return pairs.iterator();
//    }).groupByKey()
//      .mapValues((it) -> {
//        int sum = 0;
//        for (long c : it) {
//          sum += c;
//        }
//        return sum;
//      })
//      .collectAsMap();

    Map<String, Integer> count = words.flatMapToPair((d) -> {
      String[] tokens = d.split(" ");
      HashMap<String, Long> counts = new HashMap<>();
      ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
      for (String token : tokens) {
        counts.put(token, 1L + counts.getOrDefault(token, 0L));
      }
      for (Map.Entry<String, Long> e : counts.entrySet()) {
        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
      }
      return pairs.iterator();
//      return counts.entrySet().stream()
//        .map((entry) -> new Tuple2<>(entry.getKey(), entry.getValue()))
//        .iterator();
    }).groupByKey()
      .mapValues((it) -> {
        int sum = 0;
        for (long c : it) {
          sum += c;
        }
        return sum;
      })
      .collectAsMap();

//    Map<String, Long> count = words.flatMapToPair((d) -> {
//      String[] tokens = d.split(" ");
//      HashMap<String, Long> counts = new HashMap<>();
//      ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
//      for (String token : tokens) {
//        counts.put(token, 1L + counts.getOrDefault(token, 0L));
//      }
//      for (Map.Entry<String, Long> e : counts.entrySet()) {
//        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
//      }
//      return pairs.iterator();
//    }).reduceByKey((x, y) -> x + y)
//      .collectAsMap();

//    Map<String, Long> count = words.flatMap((d) -> {
//      String[] tokens = d.split(" ");
//      return Arrays.asList(tokens).iterator();
//    }).countByValue();

    long end = System.currentTimeMillis();
    System.out.println("Elapsed time: " + (end - start) + " ms");
  }

}
