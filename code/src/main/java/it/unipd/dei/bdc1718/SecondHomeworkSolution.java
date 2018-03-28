package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class SecondHomeworkSolution {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      throw new IllegalArgumentException("This program accepts just the path to the input file and an integer k");
    }

    String path = args[0];
    int k = Integer.parseInt(args[1]);

    // Setup Spark
    SparkConf conf = new SparkConf(true)
            .setMaster("local")
            .setAppName("WordCount");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> words = sc.textFile(path, 16)
            .persist(StorageLevel.MEMORY_AND_DISK());
    // The count below triggers the loading and caching of the words dataset.
    words.count();

    List<Tuple2<String, Long>> topWords;

    long start = System.currentTimeMillis();

    // Your code here
    topWords = words
            .flatMapToPair((doc) -> Arrays.stream(doc.split(" ")).map((t) -> new Tuple2<>(t, 1L)).iterator())
            .reduceByKey((c1, c2) -> c1 + c2)
            .top(k, new TupleComparator());

    long end = System.currentTimeMillis();
    System.out.println("Elapsed time: " + (end - start) + " ms");

    appendResult(path, k, end - start, topWords);

    // The following two lines make the program wait to allow
    // you to explore the web interface
    System.out.println("Press enter to finish");
//    System.in.read();
  }

  private static class TupleComparator implements Comparator<Tuple2<String, Long>>, Serializable {
    @Override
    public int compare(Tuple2<String, Long> t1, Tuple2<String, Long> t2) {
      return t1._2().compareTo(t2._2());
    }
  }

  private static void appendResult(String input, int k, long elapsedTime, List<Tuple2<String, Long>> top) throws IOException {
    StringBuffer topStr = new StringBuffer();
    for (Tuple2<String, Long> t : top) {
      System.out.println(t);
      topStr.append(t._1()).append(':').append(t._2()).append(';');
    }
    Files.write(
            Paths.get("word-count-result.txt"),
            (input + "," + k + "," + elapsedTime + "," + topStr.toString() + "\n").getBytes(),
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE);
  }

}