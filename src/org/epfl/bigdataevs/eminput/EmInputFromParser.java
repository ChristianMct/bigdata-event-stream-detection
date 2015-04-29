package org.epfl.bigdataevs.eminput;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
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
    timePartitions = parserInput.flatMapToPair(new ProcessArticle(partitioning))
            .groupByKey().mapToPair(new MapArticleToTimePartition());
  }
}



@SuppressWarnings("serial")
class ProcessArticle implements 
      PairFlatMapFunction<SegmentedArticle, TimePeriod, ParsedArticle> {

  private final List<TimePeriod> partitioning;
  
  public ProcessArticle(final List<TimePeriod> partitioning) {
    this.partitioning = partitioning;
  }
  
  /** Compute the count of each word in article. Then produce a ParsedArticle
   * and return a mapping of this ParsedArticle with each of its parent
   * TimePeriod partitions. **/
  public List<Tuple2<TimePeriod, ParsedArticle>> call(SegmentedArticle article) {         
    
    List<Tuple2<TimePeriod, ParsedArticle>> result = 
            new ArrayList<Tuple2<TimePeriod, ParsedArticle>>();
    
    //compute word count for this article
    HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
    for (String word : article.words) {
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
            article.publication);
    for (TimePeriod containingPeriod: partitioning) {
      if (containingPeriod.includeDates(article.publication))
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
