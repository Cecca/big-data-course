package it.unipd.dei.bd1718.admin;

import java.io.Serializable;

/**
 * Functional interface for distance functions.
 *
 * More type-specific than Function2<T, T, Double>, avoids
 * boxing/unboxing of numeric values.
 *
 * There is no need to use this interface directly. It is just
 * needed to specify function signatures so that the compiler knows
 * that they accept functions with two arguments with the same type,
 * returning a double. See for instance the functions in class Distance.
 *
 * For more information about functional interfaces, see
 * https://docs.oracle.com/javase/specs/jls/se8/html/jls-9.html#jls-9.8
 */
@FunctionalInterface
public interface DistanceFunction<T> extends Serializable {

  double apply(final T a, final T b);

}
