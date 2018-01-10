package it.unipd.dei.bd1718;


import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

/**
 * Some distance functions between several types.
 */
public class Distance {

  /**
   * Cosine distance between vectors.
   */
  public static double cosineDistance(Vector a, Vector b) {
    if (a.size() != b.size()) {
      throw new IllegalArgumentException("Vectors should be in the same space");
    }
    double num = 0;
    for (int i=0; i<a.size(); i++) {
      num += a.apply(i) * b.apply(i);
    }
    double normA = Vectors.norm(a, 2);
    double normB = Vectors.norm(b, 2);

    double cosine = num / (normA * normB);
    if (cosine > 1.0) {
      // Mathematically, this should't be possible, but due to the
      // propagation of errors in floating point operations, it
      // happens
      cosine = 1;
    }
    // If you wish to use this function with vectors that only have
    // positive components, then rescale by PI/2 instead of PI
    return (Math.PI) * Math.acos(cosine);
  }

  /**
   * Friendly wrapper for the cosine distance that takes vectors with an attached identifier.
   */
  public static double cosineDistanceWithIdentifier(Tuple2<Long, Vector> a, Tuple2<Long, Vector> b) {
    return cosineDistance(a._2(), b._2());
  }

}