package org.epfl.bigdataevs.eminput;

import org.apache.commons.math3.fraction.BigFraction;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** This class is encapsulates the input for the HMM algorithm from the parser.
 * The word stream is a RDD(Long,Long) where first Long is the word ID in the 
 * lexicon, and the second Long is it's timestamp. It also contains the BackgroundModel
 * object for the timeFrame. The InputParser is responsible for creating this object.
 * @author Christian
 */
@SuppressWarnings("serial")
public class HmmInputFromParser implements Serializable {
  
  public final BackgroundModel backgroundModel;
  public final JavaPairRDD<Long, Long> wordStream;
  public final JavaPairRDD<String, Long> lexicon;
  public final JavaPairRDD<Long, Double> backgroundModelById;
  
  /** Basic constructor for this class.
   * @param backgroundModel the backgroundModel object corresponding to the timeFrame
   * @param segmentedArticles The RDD containing all SegmentedArticles in the timeFrame
   * @param timeFrame The considered timeFrame in the dataset.
   */
  @SuppressWarnings("serial")
  public HmmInputFromParser(BackgroundModel backgroundModel,
         JavaRDD<SegmentedArticle> segmentedArticles,
         TimePeriod timeFrame) {
    
    this.backgroundModel = backgroundModel;
    
    
    JavaPairRDD<Tuple2<String, BigFraction>, Long> bgModelWithId = backgroundModel
                                                                .backgroundModelRdd
                                                                .zipWithIndex();
    
    this.lexicon = bgModelWithId.mapToPair(
      new PairFunction<Tuple2<Tuple2<String, BigFraction>,Long>, String, Long>() {
        @Override
        public Tuple2<String, Long> call(Tuple2<Tuple2<String, BigFraction>, Long> wordEntry) 
                throws Exception {
          return new Tuple2<String,Long>(wordEntry._1._1, wordEntry._2);
        }
      }
    );
    this.backgroundModelById = bgModelWithId.mapToPair(
            new PairFunction<Tuple2<Tuple2<String, BigFraction>,Long>, Long, Double>() {
              @Override
              public Tuple2<Long, Double> call(Tuple2<Tuple2<String, BigFraction>, Long> wordEntry) 
                      throws Exception {
                return new Tuple2<Long,Double>( wordEntry._2,wordEntry._1._2.doubleValue());
              }
            }
          );
    
    // Sorts the articles by date
    segmentedArticles = segmentedArticles.sortBy(new Function<SegmentedArticle, Date>() { 
      public Date call(SegmentedArticle segArt) {
        return segArt.publication;
      } 
    }, true, segmentedArticles.partitions().size());
    
    
    // Produce the big list of words in sorted chrono order
    final Map<String, Long> inMemLexicon = this.lexicon.collectAsMap();
    
    this.wordStream = segmentedArticles.flatMapToPair(
      new PairFlatMapFunction<SegmentedArticle, Long, Long>() {
        
        public List<Tuple2<Long, Long>> call(SegmentedArticle segArt) {
          List<Tuple2<Long, Long>> result = new LinkedList<Tuple2<Long,Long>>();
          for (String word : segArt.words) {
            Long longEncoding = inMemLexicon.get(word);
            if (longEncoding != null) {              
              result.add(new Tuple2<Long,Long>(longEncoding, segArt.publication.getTime()));
            }
          }
          return result;
        }
      }
    );
  }
}
