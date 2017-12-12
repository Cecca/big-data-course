package it.unipd.dei.bd1718;

import org.apache.commons.collections.ArrayStack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.ToDoubleBiFunction;

/**
 * Sequential approximation algorithm for the remote-clique problem
 */
public class RemoteClique {

  public static <T> ArrayList<T> runSequential(final ArrayList<T> points, int k, ToDoubleBiFunction<T, T> distance) {
    final int n = points.size();
    if (k <= n) {
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
              double d = distance.applyAsDouble(points.get(i), points.get(j));
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

}
