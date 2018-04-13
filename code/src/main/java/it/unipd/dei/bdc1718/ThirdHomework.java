package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;

public class ThirdHomework {

  public static ArrayList<Vector> kCenter(ArrayList<Vector> points, int k) {
    throw new RuntimeException("Implement me!");
  }

  public static double euclidean(Vector a, Vector b) {
    return Math.sqrt(Vectors.sqdist(a, b));
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      throw new IllegalArgumentException("USAGE: ThirdHomework PATH K");
    }

    String inputPath = args[0];
    int k = Integer.parseInt(args[1]);

    ArrayList<Vector> input = InputOutput.readVectorsSeq(inputPath);
    long start = System.currentTimeMillis();
    System.out.println("Loaded input with " + input.size() + " points");
    ArrayList<Vector> solution = kCenter(input, k);
    long end = System.currentTimeMillis();

    if (solution.size() > k) {
      throw new IllegalArgumentException(
              "The solution has " + solution.size() + " points ( > " + k + " )");
    }

    long elapsed = end - start;
    System.out.println("Elapsed time " + elapsed + " ms");
  }


}
