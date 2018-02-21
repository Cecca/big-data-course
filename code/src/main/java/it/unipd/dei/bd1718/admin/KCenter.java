package it.unipd.dei.bd1718.admin;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Implementation of Gonzalez's k-center algorithm.
 */
public class KCenter {

  public static <T> ArrayList<T> run(final ArrayList<T> points, final int k, DistanceFunction<T> distance) {
    final int n = points.size();
    if (n < k) {
      throw new IllegalArgumentException("Cannot compute clustering with " + k + " clusters on " + n + " points");
    } else if (n == k) {
      return points;
    }

    double[] minDistances = new double[n];
    Arrays.fill(minDistances, Double.POSITIVE_INFINITY);

    ArrayList<T> centers = new ArrayList<>(k);

    T lastCenter = points.get(0);
    centers.add(lastCenter);

    for (int iter=1; iter<k; iter++) {
      int maxIdx = 0;
      double maxDist = 0;

      for (int i=0; i<n; i++) {
        double d = distance.apply(points.get(i), lastCenter);
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

}
