package it.unipd.dei.bd1718;

import java.io.Serializable;

/**
 * Functional interface for distance functions.
 *
 * More type-specific than Function2<T, T, Double>, avoids
 * boxing/unboxing of numeric values.
 */
public interface DistanceFunction<T> extends Serializable {

  double apply(final T a, final T b);

}
