package org.epfl.bigdataevs.em;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.fraction.BigFraction;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePeriod;

import scala.Tuple2;

/**Team: Matias and Christian.
*EmInput: container for the RDDs representing the background
*model and the word distribution of every article, for all streams.
*Please note that the two JavaPairRDD attributes are not key-value maps,
*just lists of tuples.
*TODO: should the background model be per-stream?
**/

public class EmInput implements Serializable {
  /** HashMap containing tuples of words and their 
   * distribution in the streams. **/
  public HashMap<String, Double> backgroundModel;
  /** List containing articles published at that time. **/
  public ArrayList<ParsedArticle> parsedArticles;
  
  /** List of the themes appearing in this input*/
  public ArrayList<Theme> themesOfPartition;
  
  public Long indexOfPartition = 0L;
  
  public int run;
  
  public TimePeriod timePeriod;
  
  
  public EmInput(HashMap<String, Double> backgroundModel,
          ArrayList<ParsedArticle> parsedArticles, TimePeriod period) {
    
    this.backgroundModel = backgroundModel;
    this.parsedArticles = parsedArticles;
    this.themesOfPartition = new ArrayList<>();
    this.timePeriod = period;
  }
  
  public void addTheme(Theme theme) {
    this.themesOfPartition.add(theme);
  }
  
  public void initializeArticlesProbabilities() {
    for (ParsedArticle article : this.parsedArticles) {
      article.initializeProbabilities(themesOfPartition);
    }
  }
  
  /**
   * Compute the log-likelihood of the mixture model
   * @return log-likelohood
   */
  public double computeLogLikelihood(double lambdaBackgroundModel) {
    double logLikelihood = 0.0;
    for (ParsedArticle parsedArticle : parsedArticles) {
      for (String word : parsedArticle.words.keySet()) {
        double temp = 0.0;
        for (Theme theme : themesOfPartition) {
          temp = temp + (parsedArticle.probabilitiesDocumentBelongsToThemes.get(theme)
                  * theme.wordsProbability.get(word));
        }
        logLikelihood += parsedArticle.words.get(word) * Math.log(
                (lambdaBackgroundModel * backgroundModel.get(word))
                + ((1.0 - lambdaBackgroundModel) * temp));
      }
    }
    return logLikelihood;
  }
  
  /**
   * Update probabilities word belongs to theme
   */
  public Double subUpdateProbabilitiesOfWordsGivenTheme(String word, Theme theme) {
    double value  = 0.0;
    for (ParsedArticle parsedArticle : parsedArticles) {
      if (parsedArticle.words.containsKey(word)) {
        value = value + (((double) parsedArticle.words.get(word))
                * (1.0 - parsedArticle.probabilitiesHiddenVariablesBackgroundModel.get(word)) 
                        * (parsedArticle.probabilitiesHiddenVariablesThemes.get(
                                Pair.of(word, theme))));
      }
    }
    return value;
  }
  
  public void updateProbabilitiesOfWordsGivenTheme(ArrayList<Theme> themes) {
   
    for (Theme theme : themes) {
      double denominator = 0.0;
      for (String word : theme.wordsProbability.keySet()) {
        denominator = denominator + subUpdateProbabilitiesOfWordsGivenTheme(word, theme);
      }
      for (String word : theme.wordsProbability.keySet()) {
        double numerator = subUpdateProbabilitiesOfWordsGivenTheme(word, theme);
        theme.wordsProbability.put(word, numerator / (denominator + EmAlgo.epsilon));
      }
    }
    
  }
  
  public EmInput clone() {
    ArrayList<ParsedArticle> articles = new ArrayList<>();
    for (ParsedArticle parsedArticle : this.parsedArticles) {
      articles.add(new ParsedArticle(parsedArticle.words, parsedArticle.stream));
    }
    return new EmInput(this.backgroundModel, articles, this.timePeriod);
  }
}
