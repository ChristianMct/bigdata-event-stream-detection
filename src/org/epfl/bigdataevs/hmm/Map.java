package org.epfl.bigdataevs.hmm;

public final class Map<T extends PubliclyCloneable<T>> {

  public void map( T[] inOutArray, MapOperator<T> op ) {
    int size = inOutArray.length;
    for (int i = 0; i < size; i++ ) {
      inOutArray[i] = op.apply(i, inOutArray[i], inOutArray[i].publicClone());
    }
  }
}
