package org.epfl.bigdataevs.eminput;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.commons.math3.fraction.Fraction;

import scala.Tuple2;

/**Team: Matias and Christian.
 *TimePartition: container representing the background
 *model and the word distribution of every article, for all streams.
 *TODO: assert that the size of these data structure is not problematic
 **/
public class TimePartition {

  /** Map containing tuples of words and their 
   * distribution in the streams. */
  public final Map<String, Fraction> backgroundModel;


  /** Collection containing tuples of timestamps and the (processed)
   * articles published at that time. */
  public final Collection<ParsedArticle> parsedArticles;


  /** The time period on which this TimePartition is based. */
  public final TimePeriod timePeriod;


  /**Initializer from a background model and a collection of ParsedArticle.
   * @param backgroundModel the background model based on all article in the TimePeriod
   * @param parsedArticles all the ParsedArticles in the TimePeriod.
   * @param forTimePeriod the TimePeriod on which this TimePartition is based
   */
  public TimePartition(Map<String, Fraction> backgroundModel,
          Collection<ParsedArticle> parsedArticles,
          TimePeriod forTimePeriod) {
    this.timePeriod = forTimePeriod;
    this.backgroundModel = backgroundModel;
    this.parsedArticles = parsedArticles;
  }


  /** Creates a TimePartition instance from an RDD containing all articles from all streams within
   * a particular time period.
   * @input the JavaRDD of RawArticle instances, contains every articles with their full text
   * @return a TimePartition containing the background model for this time frame and the word
   *     count of every individual article. **/
  public static TimePartition generateTimePartitionModel(JavaRDD<RawArticle> input, 
          TimePeriod period) {

    //Turn RawArticle RDD into ParsedArticle RDD (see ProcessArticle.call() below)
    JavaRDD<ParsedArticle> processedArticles = input.map(new ProcessArticle());

    //temporary background model data: maps every word to its count for the whole time period
    JavaPairRDD<String, Integer> wordCountRdd = processedArticles
            .flatMapToPair(new MapParsedArticleToWordCount());

    @SuppressWarnings("serial")
    //merge all equivalent words to the same key, adding their count
    JavaPairRDD<String, Integer> 
        wordCountRddReduced = wordCountRdd
          .reduceByKey(new Function2<Integer,
                  Integer,
                  Integer>() {  
            public Integer call(Integer lhs, Integer rhs) { 
              return lhs + rhs; 
            }   
            });

    
    List<Tuple2<String, Integer>> allWordCounts = wordCountRddReduced.collect();
    int totalAmountCounter = 0;
    for (Tuple2<String, Integer> tuple : allWordCounts) {
      totalAmountCounter += tuple._2;
    }
    
    final int totalAmount = totalAmountCounter;

    //turn wordCountRDDReduced into the final backgroundModelRDD (Map words to their distribution)
    //for each [word, word-count] pair, replace with [word, Fraction(word-count, total-word-count)]
    @SuppressWarnings("serial")
    JavaPairRDD<String, Fraction>
        backgroundModelRdd = wordCountRddReduced.mapValues(new Function<Integer, Fraction>() {
          public Fraction call(Integer count) {
            return new Fraction(count, totalAmount);
          }     
          });

    return new TimePartition(backgroundModelRdd.collectAsMap(), 
                             processedArticles.collect(),
                             period);  
  }
}


@SuppressWarnings("serial")
class ProcessArticle implements Function<RawArticle, ParsedArticle> {

  /** Splits the RawArticle's text into a list of words. Then map each word to its count
   * in this article, and produce a new ParsedArticle. **/
  public ParsedArticle call(RawArticle article) {         
    HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
    //Split on anything not a letter or decimal
    String[] words = article.fullText.split("[^\\p{L}\\p{Nd}]+");

    for (String word : words) {
      //TODO: make actual cleaning!
      String cleanedWord = word.toLowerCase();

      Integer currentCount = wordCount.get(cleanedWord);
      if (currentCount == null || currentCount <= 0) {
        currentCount = 1;
      } else {
        currentCount++;
      }

      wordCount.put(cleanedWord, currentCount);
    }

    return new ParsedArticle(wordCount, article.stream);
  }
}

@SuppressWarnings("serial")
class MapParsedArticleToWordCount implements PairFlatMapFunction<ParsedArticle, String, Integer> {

  /** PairFlatMapFunction requires an iterable of [K,V] pairs. We iterate over each ParsedArticle in
   * an RDD to build a list of (word, word-count) pair **/
  public Iterable<Tuple2<String, Integer>> call(ParsedArticle article) {         
    //Turn (word->count) map into a list of pairs (word, count) and add to RDD
    LinkedList<Tuple2<String, Integer>> result = new LinkedList<Tuple2<String, Integer>>();
    for (String word : article.words.keySet()) {
      result.add(new Tuple2<String, Integer>(word, article.words.get(word)));
    }
    return result;
  }
}