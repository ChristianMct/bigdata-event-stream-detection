package org.epfl.bigdataevs.eminput;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.epfl.bigdataevs.eminput.InputParser.SegmentedArticle;

import scala.Tuple2;

import java.util.LinkedList;

public class BackgroundModel {
  
  public final JavaPairRDD<String, Fraction> backgroundModelRdd;

  @SuppressWarnings("serial")
  public BackgroundModel(JavaRDD<SegmentedArticle> segmentedArticles) {
    
    JavaPairRDD<String, Integer> wordCount = createWordCountRdd(segmentedArticles);   
    
    // Counts the total number of word in all segmentedArticles
    // TODO : check que int va pas overflow
    final int totalAmount = wordCount.reduce(new Function2<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>() {
      @Override
      public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
        return new Tuple2<String, Integer>("acc", v1._2 + v2._2);
      }
    })._2;
    
    // Create the backgroundModel RDD
    backgroundModelRdd = wordCount.mapValues(new Function<Integer, Fraction>() {
      public Fraction call(Integer count) {
        return new Fraction(count, totalAmount);
      }     
    });
  }
  
  /** Returns RDD mapping each (cleaned) word to its count in the whole dataset. **/
  @SuppressWarnings("serial")
  private JavaPairRDD<String, Integer> 
        createWordCountRdd(JavaRDD<SegmentedArticle> data) {
    
    //temporary background model data: maps every word to its count for the whole time period
    JavaPairRDD<String, Integer> wordCountRdd = data
            .flatMapToPair(new PairFlatMapFunction<SegmentedArticle, String, Integer>() {
              
              public Iterable<Tuple2<String, Integer>> call(SegmentedArticle art) {
                LinkedList<Tuple2<String, Integer>> countTuples = 
                        new LinkedList<Tuple2<String, Integer>>();
                for (String word : art.words) {
                  countTuples.add(new Tuple2<String, Integer>(word, 1));
                }
                return countTuples;                
              }
              
            });
    
    //merge all equivalent words to the same key, adding their count
    JavaPairRDD<String, Integer> wordCountRddReduced = 
              wordCountRdd.reduceByKey(new Function2<Integer,
                  Integer,
                  Integer>() {  
                public Integer call(Integer lhs, Integer rhs) { 
                  return lhs + rhs; 
                }   
              });
    
    return wordCountRddReduced;
  }
  
}
