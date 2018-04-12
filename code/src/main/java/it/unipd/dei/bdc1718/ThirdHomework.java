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

public class ThirdHomework {

  public static ArrayList<Vector> kCenter(ArrayList<Vector> points, int k) {
    throw new RuntimeException("Implement me!");
  }

  public static double euclidean(Vector a, Vector b) {
    return Math.sqrt(Vectors.sqdist(a, b));
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

    ArrayList<Vector> input = InputOutput.readVectorsSequential(arguments.input);
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
