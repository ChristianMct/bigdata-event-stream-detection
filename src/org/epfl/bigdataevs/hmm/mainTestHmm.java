package org.epfl.bigdataevs.hmm;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class mainTestHmm {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
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
            { 0.1, 0.8, 0.1, 0.0, 0 }, { 0, 0, 0.8, 0.2, 0.0 } };

    Hmm hmm = new Hmm(output, pi,a, b);

    List<String> observationSequence = hmm.generateObservationSequence(20000000);
    int[] hiddenSequence = hmm.decode(observationSequence);
    System.out.println("done decoding");
    
    // Test HMM training
    int n = pi.length;
    int m = 5;
    double[] initialPi = {0.25,0.25,0.25,0.25};
    double[][] initialA = { { 0.25, 0.25, 0.25, 0.25 }, { 0.25, 0.25, 0.25, 0.25 }, { 0.25, 0.25, 0.25, 0.25 },
            { 0.25, 0.25, 0.25, 0.25 } };
    Hmm trainedHmm = new Hmm(n, m, initialPi, initialA, b);
    
    int[] rawSequence = hmm.generateRawObservationSequence(1000);
    int seqSize = 100;
    while ( seqSize <= 1000) {
      trainedHmm.rawTrain(rawSequence, seqSize);
      
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
