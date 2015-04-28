package org.epfl.bigdataevs.eminput;

import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.epfl.bigdataevs.eminput.InputParser.SegmentedArticle;

import java.sql.Time;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class HmmInputFromParser {
  
  public final BackgroundModel backgroundModel;
  public final JavaRDD<String> wordStream;
  //public final UnmodifiableList counts;
  
  @SuppressWarnings("serial")
  public HmmInputFromParser(BackgroundModel backgroundModel,
         JavaRDD<SegmentedArticle> segmentedArticles,
         TimePeriod timeFrame,
         Time dt) {
    
    this.backgroundModel = backgroundModel;
    
    // Sorts the articles by date
    segmentedArticles = segmentedArticles.sortBy(new Function<SegmentedArticle, Date>() { 
      public Date call(SegmentedArticle segArt) {
        return segArt.publication;
      } 
    }, true, segmentedArticles.partitions().size());
    
    List<Integer> counts = new LinkedList<Integer>();
    // Produce the big list of words in sorted chrono order
    this.wordStream = segmentedArticles.flatMap(
      new FlatMapFunction<SegmentedArticle, String>() {
        
        public List<String> call(SegmentedArticle segArt) {
          return segArt.words;
        }
      }
    );
  
    //TODO: Find a way to put time markers in the wordStream: suggests -> special string in the RDD !!
    
  }
}
