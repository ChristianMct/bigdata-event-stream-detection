package org.epfl.bigdataevs.hmm;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class mainTestHmm {
  
  public static void main(String[] args) {
    // test scan left
    int mSize = 4;
    SquareMatrix[] array = {
            new SquareMatrix(mSize).setIdentity().set(0, 0, 2.0),
            new SquareMatrix(mSize).setIdentity(),
            new SquareMatrix(mSize).setIdentity().set(1, 1, 2.0),
            new SquareMatrix(mSize).setIdentity(),
            new SquareMatrix(mSize).setIdentity().set(2, 2, 2.0),
            new SquareMatrix(mSize).setIdentity(),
            new SquareMatrix(mSize).setIdentity().set(2, 3, 2.0)};
    
    ScanLeft<SquareMatrix> scanner = new ScanLeft<SquareMatrix>( array.length );
    scanner.scan(
            array,
            new ReverseMatrixMultiplicationOperator(),
            new SquareMatrix(mSize).setIdentity());
    
    for ( int i = 0; i < array.length; i++ ) {
      System.out.println("[" + i + "] = { ");
      SquareMatrix matrix = array[i];
      for ( int j = 0; j < mSize; j++ ) {
        for ( int k = 0; k < mSize; k++ ) {
          System.out.print(matrix.elements[j * mSize + k] + " ");
        }
        System.out.println("");
      }
      System.out.println("}");
    }
    System.out.println("");
    
    // HMM tests
    boolean testsHMM = false;
    if( testsHMM ) {
      List<String> output = new LinkedList<String>();
      output.add("alpha");
      output.add("beta ");
      output.add("gamma");
      output.add("delta");
      output.add("epsil");
      double[] pi = {1.0,0.0,0.0,0.0};
      double[][] a = { { 0.5, 0.3, 0.1, 0.1 }, { 0.8, 0.2, 0, 0 }, { 0.7, 0, 0.3, 0 },
              { 0.2, 0, 0, 0.8 } };
  
      double[][] b = { { 0.0, 0.0, 0.0, 0.0, 1.0 }, { 0.8, 0.1, 0.1, 0, 0 },
              { 0.1, 0.8, 0.1, 0.0, 0.0 }, { 0, 0, 0.8, 0.2, 0.0 } };
  
      Hmm hmm = new Hmm(output, pi,a, b);
  
      List<String> observationSequence = hmm.generateObservationSequence(200);
      int[] hiddenSequence = hmm.decode(observationSequence);
      System.out.println("done decoding");
      
      // Test HMM training
      int n = pi.length;
      int m = 5;
      double[] initialPi = {0.25,0.25,0.25,0.25};
      double[] initialPi2 = {0.25,0.25,0.25,0.25};
      double[][] initialA = { { 0.25, 0.25, 0.25, 0.25 }, { 0.25, 0.25, 0.25, 0.25 }, { 0.25, 0.25, 0.25, 0.25 },
              { 0.25, 0.25, 0.25, 0.25 } };
      double[][] initialA2 = { { 0.25, 0.25, 0.25, 0.25 }, { 0.25, 0.25, 0.25, 0.25 }, { 0.25, 0.25, 0.25, 0.25 },
              { 0.25, 0.25, 0.25, 0.25 } };
      Hmm trainedHmm = new Hmm(n, m, initialPi, initialA, b);
      Hmm trainedHmm2 = new Hmm(n, m, initialPi2,
             initialA2, Arrays.copyOf(b,b.length));
      
      int[] rawSequence = hmm.generateRawObservationSequence(1000000);
      int seqSize = 1000000;
      while ( seqSize <= 1000000) {
        trainedHmm.rawParalellTrain(rawSequence, 0.1);
        trainedHmm2.rawTrain(rawSequence, 1000000);
     // Print Pi first
        double[] trainedPi = trainedHmm.getPi();
        System.out.println("Pi: ");
        for ( int i = 0; i < n; i++ ) {
          System.out.print(" " + trainedPi[i]);
        }
        System.out.println("");
        
        //  Print A then
        double[][] trainedA = trainedHmm.getA();
        System.out.println("A: ");
        for ( int i = 0; i < n; i++ ) {
          for (int j = 0; j < n; j++ ) {
            System.out.print(" " + trainedA[i][j]);
          }
          System.out.println("");
        }
        
        //  Print B
        double[][] trainedB = trainedHmm.getB();
        System.out.println("B: ");
        for ( int i = 0; i < n; i++ ) {
          for (int j = 0; j < m; j++ ) {
            System.out.print(" " + trainedB[i][j]);
          }
          System.out.println("");
        }
        
        
        
        
        double[] trainedPi2 = trainedHmm2.getPi();
        System.out.println("Pi2: ");
        for ( int i = 0; i < n; i++ ) {
          System.out.print(" " + trainedPi2[i]);
        }
        System.out.println("");
        
        //  Print A then
        double[][] trainedA2 = trainedHmm2.getA();
        System.out.println("A2: ");
        for ( int i = 0; i < n; i++ ) {
          for (int j = 0; j < n; j++ ) {
            System.out.print(" " + trainedA2[i][j]);
          }
          System.out.println("");
        }
        
        //  Print B
        double[][] trainedB2 = trainedHmm2.getB();
        System.out.println("B2: ");
        for ( int i = 0; i < n; i++ ) {
          for (int j = 0; j < m; j++ ) {
            System.out.print(" " + trainedB2[i][j]);
          }
          System.out.println("");
        }
        
        seqSize *= 10;
      }
      /*
      String concat = "";
      for(String os : observationSequence)
        concat += os+" ";
      String states = "";
      for(int i:hiddenSequence)
        states += i+"     ";
      System.out.println(concat);
      System.out.println(states);
      */
    }

  }
  
  

}
