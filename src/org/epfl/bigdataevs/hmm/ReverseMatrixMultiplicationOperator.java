package org.epfl.bigdataevs.hmm;

public class ReverseMatrixMultiplicationOperator implements BinaryOperator<SquareMatrix>{

  @Override
  public SquareMatrix apply(SquareMatrix first, SquareMatrix second, SquareMatrix out) {
    return second.multiplyOut( first, out );
  }

}
