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
  
  
  public EmInput(HashMap<String, Fraction> backgroundModel,
          ArrayList<ParsedArticle> parsedArticles) {
    
    this.backgroundModel = backgroundModel;
    this.parsedArticles = parsedArticles;
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
   * Update probabilities word belongs to theme
   */
  public Fraction subUpdateProbabilitiesOfWordsGivenTheme(String word, Theme theme) {
    Fraction value  = Fraction.ZERO;
    for (ParsedArticle parsedArticle : parsedArticles) {
      value.add(new Fraction(parsedArticle.words.get(word)).multiply(
              Fraction.ONE.subtract(parsedArticle.probabilitiesHiddenVariablesBackgroundModel.get(word))).multiply(
                      parsedArticle.probabilitiesHiddenVariablesThemes.get(Pair.of(word, theme))));
    }
    return value;
  }
  
  public void updateProbabilitiesOfWordsGivenTheme(ArrayList<Theme> themes) {
   
    for (Theme theme : themes) {
      Fraction denominator = Fraction.ZERO;
      for(String word : theme.wordsProbability.keySet()) {
        denominator.add(subUpdateProbabilitiesOfWordsGivenTheme(word, theme));
      }
      for (String word : theme.wordsProbability.keySet()) {
        Fraction numerator = subUpdateProbabilitiesOfWordsGivenTheme(word, theme);
        theme.wordsProbability.put(word, numerator.divide(denominator));
      }
    }
    
  }
}
