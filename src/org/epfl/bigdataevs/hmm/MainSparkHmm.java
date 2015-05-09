package org.epfl.bigdataevs.hmm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;


/**Test class for HMM based algos with generated sequences.
 * 
 * @author laurent
 *
 */
public class MainSparkHmm {

  public static void main(String[] args) {
    
    double[] pi = {1.0,0.0,0.0,0.0};
    double[][] a = { { 0.5, 0.3, 0.1, 0.1 }, { 0.8, 0.2, 0, 0 }, { 0.7, 0, 0.3, 0 },
            { 0.2, 0, 0, 0.8 } };

    double[][] b = { { 0.0, 0.0, 0.0, 0.0, 1.0 }, { 0.8, 0.1, 0.1, 0, 0 },
            { 0.1, 0.8, 0.1, 0.0, 0.0 }, { 0, 0, 0.8, 0.2, 0.0 } };
    
    int N = pi.length;
    int M = b[0].length;
    double[] initialPi = {0.25,0.25,0.25,0.25};
    double[][] initialA = { { 0.25, 0.25, 0.25, 0.25 }, { 0.25, 0.25, 0.25, 0.25 }, { 0.25, 0.25, 0.25, 0.25 },
            { 0.25, 0.25, 0.25, 0.25 } };
    Hmm realHmm = new Hmm(N, M, pi, a, b);
    Hmm2 sparkTrainedHmm2 = new Hmm2(N, M, initialPi, initialA, b);
    
    //if local test
    //JavaSparkContext sc = new JavaSparkContext("local", "Baum-Welch Test");
    
    //if test on cluster
    SparkConf sparkConf = new SparkConf().setAppName("Baum-Welch on generated sequence");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    
    int seqSize = 50;
    int[] rawSequence = realHmm.generateRawObservationSequence(seqSize);
    System.out.println("done generating sequence");
    ArrayList<Tuple2<Integer, Integer>> rawSequenceList =
            new ArrayList<Tuple2<Integer, Integer>>(seqSize);
    
    for ( int i = 0; i < seqSize; i++ ) {
      rawSequenceList.add(
              new Tuple2<Integer, Integer>(
                      new Integer(i),
                      new Integer(rawSequence[i])));
    }
    
    JavaRDD<Tuple2<Integer, Integer>> rawSequenceRdd = sc.parallelize(rawSequenceList);
    rawSequence = null;
    rawSequenceList = null;
    System.gc();
    System.out.println("done converting sequence into rdd");
    
    int maxIterations = 50;
    
    System.out.println("training using Baum-Welch, maxIterations :"+maxIterations);
    
    sparkTrainedHmm2.rawSparkTrain(sc, rawSequenceRdd, 0.0001, 0.0001, maxIterations, rawSequence);
    
    //Printing results
    // Print Pi first
    double[] trainedPi = sparkTrainedHmm2.getPi();
    System.out.println("Pi: ");
    for ( int i = 0; i < N; i++ ) {
      System.out.print(" " + trainedPi[i]);
    }
    System.out.println("");
    
    //  Print A then
    double[][] trainedA = sparkTrainedHmm2.getA();
    System.out.println("A: ");
    for ( int i = 0; i < N; i++ ) {
      for (int j = 0; j < N; j++ ) {
        System.out.print(" " + trainedA[i][j]);
      }
      System.out.println("");
    }
    
    //  Print B
    double[][] trainedB = sparkTrainedHmm2.getB();
    System.out.println("B: ");
    for ( int i = 0; i < N; i++ ) {
      for (int j = 0; j < N; j++ ) {
        System.out.print(" " + trainedB[i][j]);
      }
      System.out.println("");
    }
    
    
    JavaPairRDD<Integer,Integer> decodedStreamRdd = sparkTrainedHmm2.decode(sc, rawSequenceRdd, 1024*32) ;
    
    System.out.println("DecodedStream : "+Arrays.toString(Arrays.copyOf(decodedStreamRdd.collect().toArray(),50)));
    

  }

}
