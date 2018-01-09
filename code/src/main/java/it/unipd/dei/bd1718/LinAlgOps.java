package it.unipd.dei.bd1718;

import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;

/**
 * Linear algebra operations.
 */
public class LinAlgOps {

  public static Vector sum(Vector a, Vector b) {
    double[] c = new double[a.size()];
    for(int i=0; i<a.size(); i++) {
      c[i] = a.apply(i) + b.apply(i);
    }
    return new DenseVector(c);
  }

}
