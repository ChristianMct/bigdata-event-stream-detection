package org.epfl.bigdataevs.em;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.fraction.Fraction;
import org.epfl.bigdataevs.eminput.ParsedArticle;

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
  public HashMap<String, Fraction> backgroundModel;
  /** List containing articles published at that time. **/
  public ArrayList<ParsedArticle> parsedArticles;
  
  /** List of the themes appearing in this input*/
  public ArrayList<Theme> themesOfPartition;
  
  public Date startDate;
  public Date endDate;
  
  
  public EmInput(HashMap<String, Fraction> backgroundModel,
          ArrayList<ParsedArticle> parsedArticles, Date start, Date end) {
    
    this.backgroundModel = backgroundModel;
    this.parsedArticles = parsedArticles;
    this.themesOfPartition = new ArrayList<>();
    this.startDate = start;
    this.endDate = end;
  }
  
  public void addTheme(Theme theme) {
    this.themesOfPartition.add(theme);
    System.out.println("Theme added");
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
        Fraction temp = Fraction.ZERO;
        for (Theme theme : themesOfPartition) {
          temp.add(parsedArticle.probabilitiesDocumentBelongsToThemes.get(theme).multiply(theme.wordsProbability.get(word)));
        }
        logLikelihood += parsedArticle.words.get(word)*Math.log(
                new Fraction(lambdaBackgroundModel).multiply(backgroundModel.get(word)).add(
                new Fraction(1-lambdaBackgroundModel).multiply(temp)).doubleValue());
      }
    }
    return logLikelihood;
  }
  
  /**
   * Update probabilities word belongs to theme
   */
  public Fraction subUpdateProbabilitiesOfWordsGivenTheme(String word, Theme theme) {
    Fraction value  = Fraction.ZERO;
    for (ParsedArticle parsedArticle : parsedArticles) {
      if(parsedArticle.words.containsKey(word)) {
        value = value.add(new Fraction(parsedArticle.words.get(word)).multiply(
                Fraction.ONE.subtract(parsedArticle.probabilitiesHiddenVariablesBackgroundModel.get(word))).multiply(
                        parsedArticle.probabilitiesHiddenVariablesThemes.get(Pair.of(word, theme))));
      }
    }
    return value;
  }
  
  public void updateProbabilitiesOfWordsGivenTheme(ArrayList<Theme> themes) {
   
    for (Theme theme : themes) {
      Fraction denominator = Fraction.ZERO;
      for(String word : theme.wordsProbability.keySet()) {
        denominator = denominator.add(subUpdateProbabilitiesOfWordsGivenTheme(word, theme));
      }
      for (String word : theme.wordsProbability.keySet()) {
        Fraction numerator = subUpdateProbabilitiesOfWordsGivenTheme(word, theme);
        theme.wordsProbability.put(word, numerator.divide(denominator));
      }
    }
    
  }
}
