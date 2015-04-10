package org.epfl.bigdataevs.eminput;

import org.apache.commons.math3.fraction.Fraction;

import java.util.Collection;
import java.util.Map;

public class TimePartition {
  
  /**Map containing the distribution of each word in the whole time period, for every stream. **/
  public final Map<String, Fraction> backgroundModel;
  /** Collection of ParsedArticle, containing a map linking each word from this article to
   * its number of occurrences in this article.**/
  public final Collection<ParsedArticle> parsedArticles;
  
  /** Creates a new TimePartition instance (I put the javadoc because checkstyle wants me to). **/
  public TimePartition(Map<String, Fraction> backgroundModel,
          Collection<ParsedArticle> parsedArticles) {
    
    this.backgroundModel = backgroundModel;
    this.parsedArticles = parsedArticles;
  }

}
