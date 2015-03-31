package org.epfl.bigdataevs.eminput;

import java.util.Date;
import org.apache.spark.api.java.JavaPairRDD;

public class EmInput {
  public final JavaPairRDD<String, Double> backgroundModel;
  public final JavaPairRDD<Date,ParsedArticle> parsedArticles;
  
  public EmInput(JavaPairRDD<String, Double> backgroundModel,
          JavaPairRDD<Date,ParsedArticle> parsedArticles) {
    this.backgroundModel = backgroundModel;
    this.parsedArticles = parsedArticles;
  }
}
