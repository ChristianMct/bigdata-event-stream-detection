package org.epfl.bigdataevs.hmm;

public final class SquareMatrix implements PubliclyCloneable<SquareMatrix> {

  int size;
  double[] elements;
  
  public SquareMatrix( int size ) {
    this.size = size;
    elements = new double[size * size];
  }
  
  public SquareMatrix set( int row, int column, double val ) {
    elements[ row * size + column ] = val;
    return this;
  }
  
  /**
   * Return the raw norm 1 value of the matrix.
   * (I.e return the sum of all coefficients without absolute value)
   * @return The sum of all coefficients in the matrix
   */
  public double rawNorm1() {
    double res = 0.0;
    for ( int i = 0; i < size; i++ ) {
      for ( int j = 0; j < size; j++ ) {
        res += elements[ i * size + j ];
      }
    }
    
    return res;
  }
  
  /**
   * Set the matrix as the identity matrix.
   * @return reference to this matrix
   */
  public SquareMatrix setIdentity() {
    for ( int i = 0; i < size; i++ ) {
      for ( int j = 0; j < size; j++ ) {
        elements[i * size + j ] = 0.0;
      }
      elements[ i * size + i ] = 1.0;
    }
    
    return this;
  }
  
  /**
   * Performs in-place division by a scalar.
   * @param value to divide the elements with
   * @return reference to this matrix
   */
  public SquareMatrix scalarDivide( double value ) {
    for ( int i = 0; i < size; i++ ) {
      for ( int j = 0; j < size; j++ ) {
        elements[ i * size + j ] /= value;
      }
    }
    return this;
  }
  
  /**
   * performs this * other
   * @param other Right hand side matrix
   * @param outResult Matrix in which to store the result.
   */
  public SquareMatrix multiplyOut( SquareMatrix other, SquareMatrix outResult ) {
    for ( int i = 0; i < size; i++ ) {
      for ( int j = 0; j < size; j++ ) {
        double value = 0.0;
        for ( int k = 0; k < size; k++ ) {
          value += elements[ i * size + k ] * other.elements[ k * size + j];
        }
        outResult.elements[ i * size + j ] = value;
      }
    }
    
    return outResult;
  }

  @Override
  public SquareMatrix publicClone() {
    SquareMatrix cloned = new SquareMatrix(size);
    for ( int i = 0; i < size; i++ ) {
      for ( int j = 0; j < size; j++ ) {
        cloned.elements[ i * size + j ] = elements[ i * size + j];
      }
    }
    return cloned;
  }
}
