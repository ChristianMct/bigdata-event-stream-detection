package org.epfl.bigdataevs.hmm;

public class MatrixMultiplicationOperator implements BinaryOperator<SquareMatrix> {

  @Override
  public SquareMatrix apply(SquareMatrix first, SquareMatrix second, SquareMatrix out) {
    return first.multiplyOut(second, out);
  }

}
