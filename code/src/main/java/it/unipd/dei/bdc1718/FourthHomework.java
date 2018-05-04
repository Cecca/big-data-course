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

public class FourthHomework {

  /**
   * Two round MapReduce algorithm
   */
  public static ArrayList<Vector> runMapReduce(final JavaRDD<Vector> points, int k, int numBlocks) {
    throw new RuntimeException("Implement me!");
  }

  public static ArrayList<Vector> runRandom(final JavaRDD<Vector> points, int k) {
    throw new RuntimeException("Implement me!");
  }

  public static double euclidean(Vector a, Vector b) {
    return Math.sqrt(Vectors.sqdist(a, b));
  }

  /**
   * Compute the sum of pairwise distances (that is, the remote clique measure) on the given set.
   */
  public static double measure(final ArrayList<Vector> points) {
    final int n = points.size();
    double sum = 0.0;
    for (int i=0; i<n; i++) {
      for (int j=i+1; j<n; j++) {
        sum += euclidean(points.get(i), points.get(j));
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

    long start = System.currentTimeMillis();
    double[][] distanceMatrix = new double[n][n];
    for (int i = 0; i < n; i++) {
      for (int j = i+1; j < n; j++) {
        distanceMatrix[i][j] = euclidean(points.get(i), points.get(j));
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
              double d = distanceMatrix[i][j];
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

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("USAGE: <progname> input algorithm k blocks");
      System.exit(1);
    }
    String inputPath = args[0];
    String algorithm = args[1];
    int k = Integer.parseInt(args[2]);
    int blocks = Integer.parseInt(args[3]);

    if (k <= 2) {
      System.err.println("Parameter `k` must be greater than 2");
      System.exit(1);
    }

    SparkConf conf = new SparkConf(true).setAppName("diversity maximization");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Vector> input = InputOutput.readVectors(sc, inputPath).repartition(blocks).cache();
    long cnt = input.count(); // Force caching of input, so that we don't measure loading time
    System.out.println("Loaded dataset with " + cnt + " elements");

    long elapsed;
    ArrayList<Vector> solution;

    if ("sequential".equals(algorithm)) {
      ArrayList<Vector> localPoints = new ArrayList<>();
      localPoints.addAll(input.collect());
      long start = System.currentTimeMillis();
      solution = runSequential(localPoints, k);
      elapsed = System.currentTimeMillis() - start;
    } else if ("mapreduce".equals(algorithm)) {
      if (blocks < 0) {
        blocks = input.getNumPartitions();
      }
      long start = System.currentTimeMillis();
      solution = runMapReduce(input, k, blocks);
      elapsed = System.currentTimeMillis() - start;
    } else if ("random".equals(algorithm)) {
      long start = System.currentTimeMillis();
      solution = runRandom(input, k);
      elapsed = System.currentTimeMillis() - start;
    } else {
      throw new IllegalArgumentException("Unsupported algorithm " + algorithm);
    }

    if (solution.size() > k) {
      throw new IllegalArgumentException(
              "The solution has " + solution.size() + " points ( > " + k + " )");
    }

    double diversity = measure(solution);
    int solutionSize = solution.size();
    int numDistances = (solutionSize-1)*solutionSize / 2;
    double averageDistance = diversity / numDistances;

    System.out.println("Solution with diversity " + diversity + " (average distance " + averageDistance + ")");
    System.out.println("Elapsed time " + elapsed + " ms");
  }


}
