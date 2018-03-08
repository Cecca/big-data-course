package it.unipd.dei.bdc1718;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class FourthHomeworkSolution {

  /**
   * Two round MapReduce algorithm
   */
  public static ArrayList<Vector> runMapReduce(final JavaRDD<Vector> points, int k, int tau, int numBlocks) {

    // Map phase
    JavaRDD<ArrayList<Vector>> coresets = points
            .groupBy((v) -> new Random().nextInt(numBlocks))
            .values()
            .map((pointsIter) -> {
              ArrayList<Vector> localPoints = new ArrayList<>();
              for (Vector v : pointsIter) {
                localPoints.add(v);
              }
              return ThirdHomeworkSolution.kCenter(localPoints, tau);
            });

    // Reduce phase
    ArrayList<Vector> aggregatedCoreset = coresets.reduce((a, b) -> {
      ArrayList<Vector> c = new ArrayList<>(a.size()+b.size());
      c.addAll(a);
      c.addAll(b);
      return c;
    });

    System.out.println("Aggregated coreset with " + aggregatedCoreset.size() + " points");
    return runSequential(aggregatedCoreset, k);
  }

  public static ArrayList<Vector> runRandom(final JavaRDD<Vector> points, int k) {
    ArrayList<Vector> result = new ArrayList<>(k);
    result.addAll(points.takeSample(false, k));
    return result;
  }

  /**
   * Compute the sum of pairwise distances (that is, the remote clique measure) on the given set.
   */
  public static double measure(final ArrayList<Vector> points) {
    final int n = points.size();
    double sum = 0.0;
    for (int i=0; i<n; i++) {
      for (int j=i+1; j<n; j++) {
        sum += ThirdHomeworkSolution.euclidean(points.get(i), points.get(j));
      }
    }
    return sum;
  }

  /**
   * Sequential approximation algorithm based on matching.
   */
  public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k) {
    final int n = points.size();
    if (k >= n) {
      return points;
    }


    System.out.println("Populating distance matrix (requiring " + Utils.matrixMemory(n) + ")");
    long start = System.currentTimeMillis();
    double[][] distanceMatrix = new double[n][n];
    for (int i = 0; i < n; i++) {
      for (int j = i+1; j < n; j++) {
        distanceMatrix[i][j] = ThirdHomeworkSolution.euclidean(points.get(i), points.get(j));
        distanceMatrix[j][i] = distanceMatrix[i][j];
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Populated " + n + "x" + n + " matrix in " + (end - start) + " ms");

    ArrayList<Vector> result = new ArrayList<>(k);
    boolean[] candidates = new boolean[n];
    Arrays.fill(candidates, true);

    start = System.currentTimeMillis();
    for (int iter=0; iter<k/2; iter++) {
      // Find the maximum distance pair among the candidates
      double maxDist = 0;
      int maxI = 0;
      int maxJ = 0;
      for (int i = 0; i < n; i++) {
        if (candidates[i]) {
          for (int j = i+1; j < n; j++) {
            if (candidates[j]) {
              double d = distanceMatrix[i][j]; //ThirdHomeworkSolution.euclidean(points.get(i), points.get(j));
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
    end = System.currentTimeMillis();
    System.out.println("Computed matching in " + (end - start) + " ms");

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


  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "-k", required = true)
    int k;

    @Parameter(names = "--tau")
    int tau = -1;

    @Parameter(names = "--blocks")
    int blocks = -1;

    @Parameter(names = "--algorithm")
    String algorithm = "mapreduce";

    Set<String> validAlgorithms = new HashSet<>(Arrays.asList(
            "random", "mapreduce", "sequential"
    ));

  }

  private static void appendResult(Args arguments, long elapsedTime, double diversity, double average) throws IOException {
    Files.write(
            Paths.get("diversity-result.txt"),
            (arguments.input + "," + arguments.algorithm + "," + arguments.k + "," + arguments.tau + "," + arguments.blocks + "," + elapsedTime + "," + diversity + "," + average + "\n").getBytes(),
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE);
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

    SparkConf conf = new SparkConf(true).setAppName("diversity maximization");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Vector> input = InputOutput.readVectors(sc, arguments.input).repartition(Utils.getNumCores(sc.getConf())).cache();
    long cnt = input.count(); // Force caching of input, so that we don't measure loading time
    System.out.println("Loaded dataset with " + cnt + " elements");

    long elapsed;
    ArrayList<Vector> solution;

    if ("sequential".equals(arguments.algorithm)) {
      ArrayList<Vector> localPoints = new ArrayList<>();
      localPoints.addAll(input.collect());
      long start = System.currentTimeMillis();
      solution = runSequential(localPoints, arguments.k);
      elapsed = System.currentTimeMillis() - start;
    } else if ("mapreduce".equals(arguments.algorithm)) {
      if (arguments.blocks < 0) {
        arguments.blocks = input.getNumPartitions();
      }
      if (arguments.tau < 0) {
        arguments.tau = arguments.k;
      }
      long start = System.currentTimeMillis();
      solution = runMapReduce(input, arguments.k, arguments.tau, arguments.blocks);
      elapsed = System.currentTimeMillis() - start;
    } else if ("random".equals(arguments.algorithm)) {
      long start = System.currentTimeMillis();
      solution = runRandom(input, arguments.k);
      elapsed = System.currentTimeMillis() - start;
    } else {
      throw new IllegalArgumentException("Unsupported algorithm " + arguments.algorithm);
    }

    if (solution.size() > arguments.k) {
      throw new IllegalArgumentException(
              "The solution has " + solution.size() + " points ( > " + arguments.k + " )");
    }

    double diversity = measure(solution);
    double averageDistance = diversity / solution.size();

    System.out.println("Solution with diversity " + diversity + " (average distance " + averageDistance + ")");
    System.out.println("Elapsed time " + elapsed + " ms");
    appendResult(arguments, elapsed, diversity, averageDistance);
  }


}
