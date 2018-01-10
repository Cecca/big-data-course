package it.unipd.dei.bd1718;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.function.ToDoubleBiFunction;

/**
 * Approximation algorithm for the remote-clique problem
 */
public class RemoteClique {

  private static Logger logger = LoggerFactory.getLogger(RemoteClique.class);

  public static  <T> double measure(final ArrayList<T> points, DistanceFunction<T> distance) throws Exception {
    final int n = points.size();
    double sum = 0.0;
    for (int i=0; i<n; i++) {
      for (int j=i+1; j<n; j++) {
        sum += distance.apply(points.get(i), points.get(j));
      }
    }
    return sum;
  }

  public static <T> ArrayList<T> runSequential(final ArrayList<T> points, int k, DistanceFunction<T> distance) throws Exception {
    final int n = points.size();
    if (k >= n) {
      return points;
    }

    ArrayList<T> result = new ArrayList<>(k);
    boolean[] candidates = new boolean[n];
    Arrays.fill(candidates, true);

    for (int iter=0; iter<k/2; iter++) {
      // Find the maximum distance pair among the candidates
      double maxDist = 0;
      int maxI = 0;
      int maxJ = 0;
      for (int i = 0; i < n; i++) {
        if (candidates[i]) {
          for (int j = i+1; j < n; j++) {
            if (candidates[j]) {
              double d = distance.apply(points.get(i), points.get(j));
              if (d > maxDist) {
                maxDist = d;
                maxI = i;
                maxJ = j;
              }
            }
          }
        }
      }
      // Add the points maximizing the distance to the solution
      result.add(points.get(maxI));
      result.add(points.get(maxJ));
      // Remove them from the set of candidates
      candidates[maxI] = false;
      candidates[maxJ] = false;


    }

    // Add an arbitrary point to the solution, if k is odd.
    if (k % 2 != 0) {
      for (int i = 0; i < n; i++) {
        if (candidates[i]) {
          result.add(points.get(i));
          break;
        }
      }
    }

    if (result.size() != k) {
      throw new IllegalStateException("Result of the wrong size");
    }

    return result;
  }

  public static <T> ArrayList<T> runMapReduce(final JavaRDD<T> points, int k, DistanceFunction<T> distance) throws Exception {

    // Map phase
    JavaRDD<ArrayList<T>> coresets = points.mapPartitions((it) -> {
      ArrayList<T> localPoints = new ArrayList<>();
      while(it.hasNext()) {
        localPoints.add(it.next());
      }
      ArrayList<T> coreset = KCenter.run(localPoints, k, distance);
      return Collections.singleton(coreset).iterator();
    });

    // Reduce phase
    ArrayList<T> aggregatedCoreset = coresets.reduce((a, b) -> {
      a.addAll(b);
      return a;
    });

    logger.info("Aggregated coreset has {} points", aggregatedCoreset.size());

    return runSequential(aggregatedCoreset, k, distance);
  }

  public static <T> ArrayList<T> runRandom(final JavaRDD<T> points, int k) {
    ArrayList<T> result = new ArrayList<>(k);
    result.addAll(points.takeSample(false, k));
    return result;
  }

  ///////////////////////////////////////////////////////////////////////////
  ///
  /// Command line interface
  ///
  ///////////////////////////////////////////////////////////////////////////

  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "-k", required = true)
    int k;

    @Parameter(names = "--algorithm")
    String algorithm = "random";

    @Parameter(names = "--pages")
    String pagesPath = null;

    Set<String> validAlgorithms = new HashSet<>(Arrays.asList(
            "random", "mapreduce", "sequential"
    ));

  }

  public static void main(String[] args) throws Exception {

    Args arguments = new Args();
    JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

    if (!arguments.validAlgorithms.contains(arguments.algorithm)) {
      System.err.println("Unknown algorithm `" + arguments.algorithm + "`");
      System.err.println("Valid algorithms are");
      System.err.println("  " + arguments.validAlgorithms);
      System.exit(1);
    }

    if (arguments.k <= 2) {
      System.err.println("Parameter `k` must be greater than 2");
      System.exit(1);
    }

    SparkConf conf = new SparkConf(true)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .setAppName("Remote clique diversity");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Tuple2<Long, Vector>> vectors = InputOutput.readVectorsPairs(sc, arguments.input);

    ArrayList<Tuple2<Long, Vector>> solution;
    if ("random".equals(arguments.algorithm)) {
      logger.info("Running random algorithm");
      solution = runRandom(vectors, arguments.k);
    } else if ("mapreduce".equals(arguments.algorithm)) {
      logger.info("Running MapReduce algorithm");
      solution = runMapReduce(vectors, arguments.k, Distance::cosineDistanceWithIdentifier);
    } else if ("sequential".equals(arguments.algorithm)) {
      logger.info("Running sequential algorithm");
      ArrayList<Tuple2<Long, Vector>> localVectors = (ArrayList<Tuple2<Long, Vector>>) vectors.collect();
      solution = runSequential(localVectors, arguments.k, Distance::cosineDistanceWithIdentifier);
    }  else {
      throw new IllegalArgumentException("Unknown algorithm");
    }

    if (solution.size() != arguments.k) {
      throw new IllegalArgumentException("Solution of the wrong size: " + solution.size() + " instead of " + arguments.k);
    }

    if (arguments.pagesPath == null) {
      System.out.println("Solution with diversity " + measure(solution, Distance::cosineDistanceWithIdentifier) + "\n");
      for (Tuple2<Long, Vector> p : solution) {
        System.out.println("Page: " + p._1());
      }
    } else {
      JavaRDD<WikiPage> pages = InputOutput.read(sc, arguments.pagesPath);
      ArrayList<Long> ids = new ArrayList<>(solution.size());
      for (Tuple2<Long, Vector> p : solution) {
        ids.add(p._1());
      }
      List<WikiPage> matches = pages.filter((p) -> ids.contains(p.getId())).collect();
      System.out.println("Solution with diversity " + measure(solution, Distance::cosineDistanceWithIdentifier) + "\n");
      for (WikiPage p : matches) {
        System.out.println(p);
      }
    }

  }

}
