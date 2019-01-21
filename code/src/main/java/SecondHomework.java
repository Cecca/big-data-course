import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class SecondHomework {

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

    List<Tuple2<String, Long>> topWords = null;

    long start = System.currentTimeMillis();

    // Your code here

    long end = System.currentTimeMillis();
    System.out.println("Elapsed time: " + (end - start) + " ms");

    appendResult(path, k, end - start, topWords);

    // The following two lines make the program wait to allow
    // you to explore the web interface
    System.out.println("Press enter to finish");
    System.in.read();
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
