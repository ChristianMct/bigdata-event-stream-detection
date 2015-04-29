package org.epfl.bigdataevs.hmm;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class mainTestHmm {
  
  public static void main(String[] args) {
    // HMM tests
    boolean testsHMM = true;
    if ( testsHMM ) {
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
      Hmm sparkTrainedHmm = new Hmm(n, m, initialPi, initialA, b);
      Hmm trainedHmm2 = new Hmm(n, m, initialPi2,
             initialA2, Arrays.copyOf(b,b.length));
      
      JavaSparkContext sc = new JavaSparkContext("local", "EM Algorithm Test");
      
      int seqSize = 100;
      int[] rawSequence = hmm.generateRawObservationSequence(seqSize);
      
      ArrayList<Tuple2<Integer, Integer>> rawSequenceList =
              new ArrayList<Tuple2<Integer, Integer>>();
      
      for ( int i = 0; i < seqSize; i++ ) {
        rawSequenceList.add(
                new Tuple2<Integer, Integer>(
                        new Integer(i),
                        new Integer(rawSequence[i])));
      }
      
      JavaRDD<Tuple2<Integer, Integer>> rawSequenceRdd = sc.parallelize(rawSequenceList);
      
      sparkTrainedHmm.rawSparkTrain(sc, rawSequenceRdd, 0.01, 0.01, 1);
      trainedHmm2.rawTrain(rawSequence, 100);
   // Print Pi first
      double[] trainedPi = sparkTrainedHmm.getPi();
      System.out.println("Pi: ");
      for ( int i = 0; i < n; i++ ) {
        System.out.print(" " + trainedPi[i]);
      }
      System.out.println("");
      
      //  Print A then
      double[][] trainedA = sparkTrainedHmm.getA();
      System.out.println("A: ");
      for ( int i = 0; i < n; i++ ) {
        for (int j = 0; j < n; j++ ) {
          System.out.print(" " + trainedA[i][j]);
        }
        System.out.println("");
      }
      
      //  Print B
      double[][] trainedB = sparkTrainedHmm.getB();
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
