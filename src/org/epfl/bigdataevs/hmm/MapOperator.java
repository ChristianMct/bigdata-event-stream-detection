package org.epfl.bigdataevs.hmm;

public interface MapOperator<T> {

  public T apply( int index, T element, T out );
}
