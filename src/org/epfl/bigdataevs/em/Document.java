package org.epfl.bigdataevs.em;

import org.apache.spark.api.java.JavaPairRDD;
import org.epfl.bigdataevs.em.EmAlgo;
import org.epfl.bigdataevs.em.Theme;
import org.epfl.bigdataevs.eminput.ArticleStream;
import org.epfl.bigdataevs.eminput.ParsedArticle;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.*;
import org.apache.commons.math3.fraction.BigFraction;
import org.apache.commons.lang3.tuple.*;
import org.apache.hadoop.util.hash.Hash;

/**
 * Team: Antoine & Nina
 * 
 * @author antoine
 * 
 * Container for the data of processed articles. An instance 
 * of this class contains the cleaned words and their count
 * in this article, as well as the stream identifier, 
 * its title and the publication date.
 **/

public class Document implements Serializable {
  /**Maps a word to the number of times it appears in this article.**/
  public final Map<String, Integer> words;
  /**This article's stream identifier. **/
  public final ArticleStream stream;
  /**Article's title**/
  public final String title;

  
  /**
   * Probability that this document belongs to the themes, Pi(d,j)
   * */
  public HashMap<Theme, Double> probabilitiesDocumentBelongsToThemes = new HashMap<>();

  /** Hidden variable regarding themes**/
  public HashMap<Pair<String, Theme>, Double> probabilitiesHiddenVariablesThemes = new HashMap<>();
  
  /**Hidden variable regarding background model **/
  public HashMap<String, Double> probabilitiesHiddenVariablesBackgroundModel = new HashMap<>();
  
  public Document(Map<String, Integer> words, ArticleStream stream, String title) {
    this.words = words;
    this.stream = stream;
    this.title = title;
  }
  
  public Document(ParsedArticle article) {
    this.words = article.words;
    this.stream = article.stream;
    this.title = article.title;
  }
  
  /**
   * Initialize all themes probabilities from probabilitiesDocumentBelongsToThemes uniformly
   * Initialize hidden variables probabilities hashmaps.
   **/
  public void initializeProbabilities(ArrayList<Theme> themes) {
    for (int i = 0; i < themes.size(); i++) {
      double value = (1.0 / (double) themes.size());
      
      this.probabilitiesDocumentBelongsToThemes.put(themes.get(i), value);
      
      for(String word : this.words.keySet()) {
        probabilitiesHiddenVariablesThemes.put(Pair.of(word, themes.get(i)), 0.0);
      }
      
    }
    
    for (String word : this.words.keySet()) {
      probabilitiesHiddenVariablesBackgroundModel.put(word, 0.0);
    }
    
  }
  
  
  /**
   * Update hidden variables regarding themes
   */
  public void updateHiddenVariablesThemes() {
    HashMap<String,Double> denominators = new HashMap<String,Double>();
    for (Pair<String, Theme> pair : probabilitiesHiddenVariablesThemes.keySet()) {
      String word = pair.getLeft();
      if(!denominators.containsKey(word)){
        double denominator = 0.0;
        for (Theme otherTheme : probabilitiesDocumentBelongsToThemes.keySet()) {
          denominator += ((this.probabilitiesDocumentBelongsToThemes.get(otherTheme)
                  * otherTheme.wordsProbability.get(word)));
        }
        denominators.put(word,denominator);
      }
    }
    for (Pair<String, Theme> pair : probabilitiesHiddenVariablesThemes.keySet()) {
      String word = pair.getLeft();
      Theme theme = pair.getRight();
      if (this.probabilitiesHiddenVariablesBackgroundModel.get(word) == null ||
              this.probabilitiesHiddenVariablesBackgroundModel.get(word) == 1.0) {
        this.probabilitiesHiddenVariablesThemes.put(pair, 0.0);
      } else {
        double numerator = this.probabilitiesDocumentBelongsToThemes.get(theme)
                * (theme.wordsProbability.get(word));
        double denominator = denominators.get(word);
        this.probabilitiesHiddenVariablesThemes.put(
                pair, numerator / (denominator));
      }
    }
  }
  
  /**
   * Update hidden variable regarding background model
   */
  public void updateHiddenVariableBackgroundModel(Map<String, Double> backgroundModel, double lambdaB) {
    for (String word : this.probabilitiesHiddenVariablesBackgroundModel.keySet()) {
      double numerator = backgroundModel.get(word)*lambdaB;
      double temp = 0.0;
      for (Theme otherTheme : probabilitiesDocumentBelongsToThemes.keySet()) {
        temp = temp + (this.probabilitiesDocumentBelongsToThemes.get(otherTheme)
                * (otherTheme.wordsProbability.get(word)));
      }
      double denominator = numerator + ((1.0 - lambdaB) * temp);
      
      
      this.probabilitiesHiddenVariablesBackgroundModel.put(word, numerator / (denominator));
    }
   
  }
  
  /**
   * Sub function to update probabilities document belongs to themes
   */
  public Double subUpdateProbabilitiesDocumentBelongsToThemes(Theme theme) {
    double value = 0.0;
    for (String word : this.words.keySet()) {
      if (this.words.get(word) > 0) {
      value = value + (((double) this.words.get(word)) * 
              (1.0 - ((double) this.probabilitiesHiddenVariablesBackgroundModel.get(word))) * (
                      this.probabilitiesHiddenVariablesThemes.get(Pair.of(word, theme))));
      }
    }
    return value;
  }
  
  /**
   * Update probabilities document belongs to themes
   */
  public void updateProbabilitiesDocumentBelongsToThemes() {
    double denominator = 0.0;
    for (Theme theme : this.probabilitiesDocumentBelongsToThemes.keySet()) {
      denominator = denominator + subUpdateProbabilitiesDocumentBelongsToThemes(theme);  
    }
    
    for (Theme theme : this.probabilitiesDocumentBelongsToThemes.keySet()) {
      double numerator = subUpdateProbabilitiesDocumentBelongsToThemes(theme);
      this.probabilitiesDocumentBelongsToThemes.put(theme, numerator / (denominator));
    }
  }
}
