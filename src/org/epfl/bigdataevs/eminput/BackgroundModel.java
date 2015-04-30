package org.epfl.bigdataevs.eminput;

import org.apache.commons.math3.fraction.BigFraction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import java.io.Serializable;
import java.util.LinkedList;

/** Class representing a BackgroundModel. This mainly has the purpose of
 * extracting some code of the main InputParser class. It wraps an 
 * RDD(String, BigFraction) representing the word distribution for a given
 * timeFrame.
 * @author Christian
 */
@SuppressWarnings("serial")
public class BackgroundModel implements Serializable {
  
  public final JavaPairRDD<String, BigFraction> backgroundModelRdd;
  
  /** Basic constructor for the class.
   * @param segmentedArticles all the SegmentedArticle in the considered timeFrame.
   */
  public BackgroundModel(JavaRDD<SegmentedArticle> segmentedArticles) {
    
    //Usual wordcount stuff
    JavaPairRDD<String, Integer> wordCount = segmentedArticles.flatMapToPair(
        new PairFlatMapFunction<SegmentedArticle, String, Integer>() {   
          public Iterable<Tuple2<String, Integer>> call(SegmentedArticle art) {
            LinkedList<Tuple2<String, Integer>> countTuples = 
                    new LinkedList<Tuple2<String, Integer>>();
            for (String word : art.words) {
              countTuples.add(new Tuple2<String, Integer>(word, 1));
            }
            return countTuples;                
          }
        }
    );
    JavaPairRDD<String, Integer> wordCountRddReduced = 
        wordCount.reduceByKey(new Function2<Integer,Integer, Integer>() {  
          public Integer call(Integer lhs, Integer rhs) { 
            return lhs + rhs; 
          }   
        }
    );
    
    // Counts the total number of word in all segmentedArticles
    // TODO : check que int va pas overflow
    final int totalAmount = wordCountRddReduced
         .map(new Function<Tuple2<String,Integer>, Integer>(){
           @Override
           public Integer call(Tuple2<String, Integer> v1) throws Exception {
             return v1._2;
           }
         })
        .reduce(new Function2<Integer, Integer, Integer>(){
          @Override
          public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
          } 
        }
    );
        
    // Create the backgroundModel RDD
    backgroundModelRdd = wordCountRddReduced
            .mapValues(new Function<Integer, BigFraction>() {
              public BigFraction call(Integer count) {
                return new BigFraction(count, totalAmount);
              }     
            })
            .filter(new Function<Tuple2<String,BigFraction>, Boolean>() {
              BigFraction cleaningTreshold = new BigFraction(2, totalAmount);   
              @Override
              public Boolean call(Tuple2<String, BigFraction> wordEntry) throws Exception {
                return wordEntry._2.compareTo(cleaningTreshold) >= 0 ;
              }
            });
  }
}
