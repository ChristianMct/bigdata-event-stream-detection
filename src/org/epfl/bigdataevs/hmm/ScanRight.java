package org.epfl.bigdataevs.hmm;

import java.util.Vector;

public class ScanRight<T extends PubliclyCloneable<T>>{

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
  public ScanRight( int arraySize ) {
    origArraySize = arraySize;
    
    // We need log2(arraySize) buffers
    numBuffers = Integer.highestOneBit(arraySize) + 1;
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
   * Perform a right scan on the array, modifying it in-place.
   * @param inOutArray Array to scan
   * @param op (associative) operator to apply
   * @param neutral Neutral element for this operator
   */
  @SuppressWarnings("unchecked")
  public void scan( T[] inOutArray, BinaryOperator<T> op, T neutral ) {
    
    int padder = paddedArraySize - origArraySize;
    Object[] buffer0 = buffers.get(0);
    // pad it with the neutral element
    for ( int i = 0; i < padder; i++ ) {
      buffer0[i] = neutral.publicClone();
    }
    // first copy the original array into our base temp buffer
    for ( int i = padder; i < paddedArraySize; i++ ) {
      buffer0[i] = (T)inOutArray[i - padder];
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
      for (int b = numBuffers - 1; b >= 1; b-- ) {
        Object[] sourceBuffer = buffers.get(b);
        Object[] destBuffer = buffers.get(b - 1);
        
        for ( int i = 0; i < bufferSize; i++ ) {
          int leftIndex = 2 * i;
          int rightIndex = 2 * i + 1;
          
          // directly copy left index
          destBuffer[leftIndex] = sourceBuffer[i];
          
          if ( i + 1 < bufferSize ) {
            destBuffer[rightIndex] = (Object)op.apply(
                    (T)destBuffer[rightIndex],
                    (T)sourceBuffer[i + 1],
                    neutral.publicClone() );
          }
        }
        
        bufferSize <<= 1;
      }
    }
    
    // copy back results in original array
    for ( int i = 0; i < origArraySize; i++ ) {
      inOutArray[i] = (T)buffer0[i + padder];
    }
  }
}
