package it.unipd.dei.bdc1718;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;

public class ThirdHomeworkSolution {

  public static double euclidean(Vector a, Vector b) {
    return Math.sqrt(Vectors.sqdist(a, b));
  }

  public static ArrayList<Vector> kCenter(ArrayList<Vector> points, int k) {
    final int n = points.size();
    if (n < k) {
      throw new IllegalArgumentException("Cannot compute clustering with " + k + " clusters on " + n + " points");
    } else if (n == k) {
      return points;
    }

    double[] minDistances = new double[n];
    Arrays.fill(minDistances, Double.POSITIVE_INFINITY);

    ArrayList<Vector> centers = new ArrayList<>(k);

    Vector lastCenter = points.get(0);
    centers.add(lastCenter);

    for (int iter=1; iter<k; iter++) {
      int maxIdx = 0;
      double maxDist = 0;

      for (int i=0; i<n; i++) {
        double d = euclidean(points.get(i), lastCenter);
        if (d < minDistances[i]) {
          minDistances[i] = d;
        }

        if (minDistances[i] > maxDist) {
          maxDist = minDistances[i];
          maxIdx = i;
        }
      }

      lastCenter = points.get(maxIdx);
      centers.add(lastCenter);
    }

    return centers;
  }


  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "-k", required = true)
    int k;

  }

  private static void appendResult(Args arguments, long elapsedTime) throws IOException {
    Files.write(
            Paths.get("k-center-time.txt"),
            (arguments.input + "," + arguments.k + "," + elapsedTime + "\n").getBytes(),
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE);
  }

  public static void main(String[] args) throws Exception {

    Args arguments = new Args();
    JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

    ArrayList<Vector> input = InputOutput.readVectorsSequentialBin(arguments.input);
    long start = System.currentTimeMillis();
    System.out.println("Loaded input with " + input.size() + " points");
    ArrayList<Vector> solution = kCenter(input, arguments.k);
    long end = System.currentTimeMillis();

    if (solution.size() > arguments.k) {
      throw new IllegalArgumentException(
              "The solution has " + solution.size() + " points ( > " + arguments.k + " )");
    }

    long elapsed = end - start;
    System.out.println("Elapsed time " + elapsed + " ms");
    appendResult(arguments, elapsed);
  }


}
