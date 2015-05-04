package org.epfl.bigdataevs.hmm;

public final class Vector implements PubliclyCloneable<Vector> {

  public double[] elements;
  
  public Vector( int size ) {
    elements = new double[size];
  }

  @Override
  public Vector publicClone() {
    Vector cloned = new Vector(elements.length);
    for ( int i = 0; i < elements.length; i++ ) {
      cloned.elements[i] = elements[i];
    }
    return cloned;
  }
}
