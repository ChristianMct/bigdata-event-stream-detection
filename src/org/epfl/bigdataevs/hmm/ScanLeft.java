package org.epfl.bigdataevs.hmm;

import java.util.Vector;

public final class ScanLeft<T extends PubliclyCloneable<T>>{

  int origArraySize;
  int paddedArraySize;
  int numBuffers;
  Vector<Object[]> buffers;
  
  /**
   * Constructor to initialise the scan.
   * Provide the original array size so that temporary
   * buffers can be allocated only once.
   * @param arraySize Original size of the array that we want to scan
   */
  public ScanLeft( int arraySize ) {
    origArraySize = arraySize;
    
    // We need log2(arraySize)+1 buffers
    int highestOneBit = Integer.highestOneBit(arraySize);
    numBuffers = 1;
    while ( highestOneBit > 0 ) {
      numBuffers += 1;
      highestOneBit >>= 1;
    }
    // first compute the padded paddedArraySize
    paddedArraySize = 1 << (numBuffers - 1);
    
    buffers = new Vector<Object[]>(numBuffers);
    
    // initialise all the buffers
    int size = paddedArraySize;
    for ( int i = 0; i < numBuffers; i++ ) {
      buffers.add(i, new Object[size]);
      size >>= 1;
    }
  }
  
  /**
   * Perform a left scan on the array, modifying it in-place.
   * @param inOutArray Array to scan
   * @param op (associative) operator to apply
   * @param neutral Neutral element for this operator
   */
  @SuppressWarnings("unchecked")
  public void scan( T[] inOutArray, BinaryOperator<T> op, T neutral ) {
    // first copy the original array into our base temp buffer
    Object[] buffer0 = buffers.get(0);
    for ( int i = 0; i < origArraySize; i++ ) {
      buffer0[i] = (T)inOutArray[i];
    }
    // pad it with the neutral element
    for ( int i = origArraySize; i < paddedArraySize; i++ ) {
      buffer0[i] = neutral.publicClone();
    }
    
    // up-reduce phase
    {
      int bufferSize = paddedArraySize >> 1;
      for (int b = 1; b < numBuffers; b++ ) {
        Object[] buffer = buffers.get(b);
        Object[] prevBuffer = buffers.get(b - 1);
  
        for ( int i = 0; i < bufferSize; i++ ) {
          int leftIndex = 2 * i;
          int rightIndex = 2 * i + 1;
          buffer[i] = op.apply(
                  (T)prevBuffer[leftIndex],
                  (T)prevBuffer[rightIndex],
                  neutral.publicClone());
        }
  
        bufferSize >>= 1;
      }
    }
    
    // down-propagate phase
    {
      int bufferSize = 1;
      int nextBufferSize = 2;
      for (int b = numBuffers - 1; b >= 1; b-- ) {
        Object[] sourceBuffer = buffers.get(b);
        Object[] destBuffer = buffers.get(b - 1);
        
        for ( int i = 0; i < bufferSize; i++ ) {
          int leftIndex = 2 * i + 1;
          int rightIndex = 2 * i + 2;
          
          // directly copy left index
          destBuffer[leftIndex] = sourceBuffer[i];
          
          if ( rightIndex < nextBufferSize ) {
            destBuffer[rightIndex] = (Object)op.apply(
                    (T)sourceBuffer[i],
                    (T)destBuffer[rightIndex],
                    neutral.publicClone() );
          }
        }
        
        bufferSize <<= 1;
        nextBufferSize <<= 1;
      }
    }
    
    // copy back results in original array
    for ( int i = 0; i < origArraySize; i++ ) {
      inOutArray[i] = (T)buffer0[i];
    }
  }
}
