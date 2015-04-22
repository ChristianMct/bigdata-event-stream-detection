package org.epfl.bigdataevs.hmm;

public class RenormalizedReverseMatrixMulOperator implements BinaryOperator<SquareMatrix> {

  @Override
  public SquareMatrix apply(SquareMatrix first, SquareMatrix second, SquareMatrix out) {
    int size = first.size;
    
    double sum = 0.0;
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < size; j++) {
        double el = 0.0;
        for (int h = 0; h < size; h++) {
          double value = second.elements[i * size + h] * first.elements[h * size + j];
          el += value;
          sum += value;                                                                    
        }
        out.elements[i * size + j] = el;
      }
    }
    
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < size; j++) {
        out.elements[i * size + j] /= sum;
      }
    }
    
    return out;
  }

}
