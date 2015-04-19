package org.epfl.bigdataevs.em;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

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
  
  public void checkPartitionForDebuging(int iter) {
    boolean themesOk = true;
    for (Theme theme : themesOfPartition) {
      for (String word : theme.wordsProbability.keySet()) {
        if (theme.wordsProbability.get(word) > 1.0 ||
                theme.wordsProbability.get(word) < 0.0 ||
                Double.isNaN(theme.wordsProbability.get(word))) {
          themesOk = false;
        }
      }
    }
    System.out.println("Themes : "+themesOk);
    
    boolean probsDocsOk = true;
    for (ParsedArticle article : this.parsedArticles) {
      for (Theme theme : article.probabilitiesDocumentBelongsToThemes.keySet()) {
        if (article.probabilitiesDocumentBelongsToThemes.get(theme) > 1.0 ||
                article.probabilitiesDocumentBelongsToThemes.get(theme) < 0.0 ||
                Double.isNaN(article.probabilitiesDocumentBelongsToThemes.get(theme))) {
          probsDocsOk = false;
        }
      }
    }
    System.out.println("Docs : "+probsDocsOk);
    
    if(iter > 0) {
      boolean hiddenThemesOk = true;
      for (ParsedArticle article : this.parsedArticles) {
        for (Pair<String, Theme> pair : article.probabilitiesHiddenVariablesThemes.keySet()) {
          if (article.probabilitiesHiddenVariablesThemes.get(pair) > 1.0 ||
                  article.probabilitiesHiddenVariablesThemes.get(pair) < 0.0 ||
                  Double.isNaN(article.probabilitiesHiddenVariablesThemes.get(pair))) {
            hiddenThemesOk = false;
          }
        }
      }
      System.out.println("Hidden themes : "+hiddenThemesOk);
      
      boolean hiddenBgMOk = true;
      for (ParsedArticle article : this.parsedArticles) {
        for (String word: article.probabilitiesHiddenVariablesBackgroundModel.keySet()) {
          if (article.probabilitiesHiddenVariablesBackgroundModel.get(word) > 1.0 ||
                  article.probabilitiesHiddenVariablesBackgroundModel.get(word) < 0.0 ||
                  Double.isNaN(article.probabilitiesHiddenVariablesBackgroundModel.get(word))) {
            hiddenBgMOk = false;
          }
        }
      }
      System.out.println("Hidden Bg model : "+hiddenBgMOk);
    }
  }
}
