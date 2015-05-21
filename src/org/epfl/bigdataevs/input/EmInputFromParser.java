package org.epfl.bigdataevs.input;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


/** Provides the needed input for the Expectation-Maximization phase.
 * Contains a reference to the background model object and a RDD of ParsedArticle 
 * instances, which detail the word count of each article individually. **/
public class EmInputFromParser {
  
  public final BackgroundModel backgroundModel;
  public final JavaPairRDD<TimePeriod, TimePartition> timePartitions;
  
  public EmInputFromParser(BackgroundModel bg, JavaRDD<SegmentedArticle> parserInput,
          List<TimePeriod> partitioning) {
    backgroundModel = bg;
    /* For memory economy, we collect the background model's key set as a list. In the 
     * parsed article map, we use it to filter out cleaned words.*/
    HashSet<String> distinctWords = new HashSet<String>(
                bg.backgroundModelRdd.keys().collect());

    // Produces (TimePeriod, ParsedArticle) pairs, => affects an article to potentially multiple timePeriods
    JavaPairRDD<TimePeriod, ParsedArticle> timePartitionned = parserInput
            .repartition(partitioning.size()) //We could maybe find a less naive value here
            .flatMapToPair(new ProcessArticle(partitioning, distinctWords));
    
    // Produce the grouping over TimePeriod => The TimePartition
    JavaPairRDD<TimePeriod, Iterable<ParsedArticle>> grouped = timePartitionned.groupByKey();

    // Transform the groupByKey Iterable<ParsedArticle> into a TimePartition
    timePartitions = grouped.mapToPair(new MapArticleToTimePartition());
  }
}



@SuppressWarnings("serial")
class ProcessArticle implements 
      PairFlatMapFunction<SegmentedArticle, TimePeriod, ParsedArticle> {

  private final List<TimePeriod> partitioning;
  /* Set of all words that haven't been cleaned from the background model. */
  private final HashSet<String> existingWords;
  
  public ProcessArticle(final List<TimePeriod> partitioning, 
          final HashSet<String> existingWords) 
  {
    this.partitioning = partitioning;
    this.existingWords = existingWords;
  }
  
  /** Compute the count of each word in article. Then produce a ParsedArticle
   * and return a mapping of this ParsedArticle with each of its parent
   * TimePeriod partitions. **/
  public List<Tuple2<TimePeriod, ParsedArticle>> call(SegmentedArticle article) {         
    
    List<Tuple2<TimePeriod, ParsedArticle>> result = 
            new ArrayList<Tuple2<TimePeriod, ParsedArticle>>();
    
    /*compute word count for this article. Words not in the background model are ignored*/
    HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
    for (String word : article.words) {
      if (!existingWords.contains(word))
        continue;
      
      Integer currentCount = wordCount.get(word);
      if (currentCount == null || currentCount <= 0) {
        currentCount = 1;
      } else {
        currentCount++;
      }
      wordCount.put(word, currentCount);
    }
    //maps new parsedArticle instance to each of the time periods including it
    ParsedArticle parsedArticle = new ParsedArticle(wordCount, article.stream, 
            article.publication, article.title);
    for (TimePeriod containingPeriod: partitioning) {
      if (containingPeriod.includeDates(article.publication) && parsedArticle.words.size() > 50)
        result.add(new Tuple2<TimePeriod, ParsedArticle>(containingPeriod, parsedArticle));
    }
      
    return result;
  }
}

/** Creates a TimePartition for groups of ParsedArticle instances that share
 * the same time period. Return as a PairRDD<TimePeriod, TimePartition> **/
@SuppressWarnings("serial")
class MapArticleToTimePartition 
  implements PairFunction<Tuple2<TimePeriod, Iterable<ParsedArticle>>,
                 TimePeriod, TimePartition> {

  @Override
  public Tuple2<TimePeriod, TimePartition> call(
          Tuple2<TimePeriod, Iterable<ParsedArticle>> articlesByPeriod) 
          throws Exception 
  {
    TimePartition value = 
    new TimePartition(IteratorUtils.toList(articlesByPeriod._2.iterator()),
            articlesByPeriod._1);
    return new Tuple2<TimePeriod, TimePartition>(articlesByPeriod._1, value);
  }

}
