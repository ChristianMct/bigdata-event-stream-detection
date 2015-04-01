package org.epfl.bigdataevs.eminput;

import java.util.Date;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.commons.math3.fraction.Fraction;

/**Team: Matias and Christian.
*EmInput: container for the RDDs representing the background
*model and the word distribution of every article, for all streams.
*Please note that the two JavaPairRDD attributes are not key-value maps,
*just lists of tuples.
*TODO: should the background model be per-stream?
**/

public class EmInput {
  /** RDD containing tuples of words and their 
   * distribution in the streams. **/
  public final JavaPairRDD<String, Fraction> backgroundModel;
  /** RDD containing tuples of timestamps and the (processed)
   * articles published at that time. **/
  public final JavaPairRDD<Date,ParsedArticle> parsedArticles;
  
  public EmInput(JavaPairRDD<String, Fraction> backgroundModel,
          JavaPairRDD<Date,ParsedArticle> parsedArticles) {
    
    this.backgroundModel = backgroundModel;
    this.parsedArticles = parsedArticles;
  }
}
