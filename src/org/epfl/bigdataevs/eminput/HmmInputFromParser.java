package org.epfl.bigdataevs.eminput;

import org.apache.commons.math3.fraction.BigFraction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.Serializable;
import java.sql.Time;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HmmInputFromParser implements Serializable {
  
  public final BackgroundModel backgroundModel;
  public final JavaPairRDD<Long, Long> wordStream;
  public final JavaPairRDD<String, Long> lexicon;
  
  @SuppressWarnings("serial")
  public HmmInputFromParser(BackgroundModel backgroundModel,
         JavaRDD<SegmentedArticle> segmentedArticles,
         TimePeriod timeFrame,
         Time dt) {
    
    this.backgroundModel = backgroundModel;
    
    
    JavaPairRDD<Tuple2<String, BigFraction>, Long> bgWithId = backgroundModel.backgroundModelRdd.zipWithIndex();
    
    this.lexicon = bgWithId.mapToPair(new PairFunction<Tuple2<Tuple2<String, BigFraction>,Long>, String, Long>() {

      @Override
      public Tuple2<String, Long> call(Tuple2<Tuple2<String, BigFraction>, Long> t) throws Exception {
        return new Tuple2<String,Long>(t._1._1, t._2);
      }      
    });
    
    
    // Sorts the articles by date
    segmentedArticles = segmentedArticles.sortBy(new Function<SegmentedArticle, Date>() { 
      public Date call(SegmentedArticle segArt) {
        return segArt.publication;
      } 
    }, true, segmentedArticles.partitions().size());
    
    
    // Produce the big list of words in sorted chrono order
   
//    JavaRDD<String> wordStreamString = segmentedArticles.flatMap(new FlatMapFunction<SegmentedArticle, String>() {
//      @Override
//      public Iterable<String> call(SegmentedArticle t) throws Exception {
//        return t.words;
//      }
//    });
    
    System.out.println(" Size of lexicon: "+this.lexicon.count());
    
    final Map<String, Long> inMemLexicon = this.lexicon.collectAsMap();
    
    this.wordStream = segmentedArticles.flatMapToPair(
      new PairFlatMapFunction<SegmentedArticle, Long, Long>() {
        
        public List<Tuple2<Long, Long>> call(SegmentedArticle segArt) {
          List<Tuple2<Long, Long>> result = new LinkedList<Tuple2<Long,Long>>();
          for(String word : segArt.words) {
            result.add(new Tuple2<Long,Long>(inMemLexicon.get(word), segArt.publication.getTime()));
          }
          return result;
        }
      }
    );
    
  }
}
