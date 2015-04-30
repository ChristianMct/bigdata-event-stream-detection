package org.epfl.bigdataevs.eminput;

import java.util.Collection;

/**Team: Matias and Christian.
 *TimePartition: container representing the background
 *model and the word distribution of every article, for all streams.
 *TODO: assert that the size of these data structure is not problematic
 **/
public class TimePartition {


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
  public TimePartition(
          Collection<ParsedArticle> parsedArticles,
          TimePeriod forTimePeriod) {
    this.timePeriod = forTimePeriod;
    this.parsedArticles = parsedArticles;
  }


  
}


