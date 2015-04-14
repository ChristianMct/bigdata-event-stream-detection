package org.epfl.bigdataevs.eminput;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import java.util.HashMap;
import java.util.LinkedList;


/**
 * @Author Matias
    *Temporary class (just to avoid Git and interface conflicts), contains the function needed
    *to generate the background model and ParsedArticle instances from an RDD of RawArticles. 
**/
public class ArticleProcessor {

  /** Creates a TimePartition instance from an RDD containing all articles from all streams within
   * a particular time period.
   * @input the JavaRDD of RawArticle instances, contains every articles with their full text
   * @return a TimePartition containing the background model for this time frame and the word
   *     count of every individual article. **/
  public static TimePartition generateTimePartitionModel(JavaRDD<RawArticle> input, TimePeriod period) {
  
    //Turn RawArticle RDD into ParsedArticle RDD (see ProcessArticle.call() below)
    JavaRDD<ParsedArticle> processedArticles = input.map(new ProcessArticle());
    
    //temporary background model data: maps every word to its count for the whole time period
    JavaPairRDD<String, Integer> wordCountRdd = processedArticles.flatMapToPair(new MapParsedArticleToWordCount());
     //merge all equivalent words to the same key, adding their count
    JavaPairRDD<String, Integer> wordCountRddReduced = wordCountRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {  
      public Integer call(Integer a, Integer b) { 
        return a + b; 
      }   
    });
        
     //count total amount of words using fold (needed for background model)
    Tuple2<String, Integer> initialValue = new Tuple2<String, Integer>("", 0);
    final Tuple2<String, Integer> totalAmount = wordCountRddReduced.fold(initialValue, new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {  
      public Tuple2<String, Integer> call(Tuple2<String, Integer> a, Tuple2<String, Integer> b) { 
        return new Tuple2<String, Integer>("", a._2 + b._2);
      } 
       
    });
    
     //Finally, turn wordCountRDDReduced into the final backgroundModelRDD (Map words to their distribution)
     //for each [word, word-count] pair, replace with [word, Fraction(word-count, total-word-count)]
     JavaPairRDD<String, Fraction> backgroundModelRdd = wordCountRddReduced.mapValues(new Function<Integer, Fraction>() {
       public Fraction call(Integer count) {
         return new Fraction(count, totalAmount._2);
       }     
     });
     
    return new TimePartition(backgroundModelRdd.collectAsMap(), processedArticles.collect(), period);  
  }

}

@SuppressWarnings("serial")
class ProcessArticle implements Function<RawArticle, ParsedArticle> {
  
  /** Splits the RawArticle's text into a list of words. Then map each word to its count
   * in this article, and produce a new ParsedArticle. **/
  public ParsedArticle call(RawArticle article) {         
    HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
    String[] words = article.fullText.split("\\s+");
    
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
