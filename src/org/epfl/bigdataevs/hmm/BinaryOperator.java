package org.epfl.bigdataevs.hmm;

public interface BinaryOperator<T> {

  public T apply( T first, T second, T out);
}
