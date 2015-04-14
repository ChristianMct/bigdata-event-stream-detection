package org.epfl.bigdataevs.eminput;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.fraction.Fraction;
import org.epfl.bigdataevs.em.Theme;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**Team: Matias and Christian.
 * Container for the data of processed articles. An instance 
 * of this class contains the cleaned words and their count
 * in this article, as well as the stream identifier.**/
public class ParsedArticle implements Serializable{
  /**Maps a word to the number of times it appears in this article.**/
  public final Map<String, Integer> words;
  /**This article's stream identifier. **/
  public final ArticleStream stream;
  /**
   * Probability that this document belongs to the themes, Pi(d,j)
   * */
  public HashMap<Theme, Fraction> probabilitiesDocumentBelongsToThemes = new HashMap<>();
  
  /** Hidden variable regarding themes**/
  public HashMap<Pair<String, Theme>, Fraction> probabilitiesHiddenVariablesThemes = new HashMap<>();
  
  /**Hidden variable regarding background model **/
  public HashMap<String, Fraction> probabilitiesHiddenVariablesBackgroundModel = new HashMap<>();
  
public ParsedArticle( Map<String, Integer> words, ArticleStream stream) {
    this.words = words;
    this.stream = stream;
  }
  
  /**
   * Initialize all themes probabilities from probabilitiesDocumentBelongsToThemes uniformly
   * Initialize hidden variables probabilities hashmaps.
   **/
  public void initializeProbabilities(ArrayList<Theme> themes) {
    for (int i = 0; i < themes.size(); i++) {
      this.probabilitiesDocumentBelongsToThemes.put(themes.get(i), new Fraction(1, themes.size()));
      for(String word : this.words.keySet()) {
        probabilitiesHiddenVariablesThemes.put(Pair.of(word, themes.get(i)), null);
      }
    }
    for (String word : this.words.keySet()) {
      probabilitiesHiddenVariablesBackgroundModel.put(word, null);
    }
  }
  
  
  /**
   * Update hidden variables regarding themes
   */
  public void updateHiddenVariablesThemes() {
    for (Pair<String, Theme> pair : probabilitiesHiddenVariablesThemes.keySet()) {
      String word = pair.getLeft();
      Theme theme = pair.getRight();
      
      Fraction numerator = this.probabilitiesDocumentBelongsToThemes.get(theme).multiply(theme.wordsProbability.get(word));
      Fraction denominator = Fraction.ZERO;
      for (Theme otherTheme : probabilitiesDocumentBelongsToThemes.keySet()) {
        denominator.add(this.probabilitiesDocumentBelongsToThemes.get(otherTheme).multiply(otherTheme.wordsProbability.get(word)));
      }
      
      this.probabilitiesHiddenVariablesThemes.put(pair, numerator.divide(denominator));
    }
  }
  
  /**
   * Update hidden variable regarding background model
   */
  public void updateHiddenVariableBackgroundModel(HashMap<String, Fraction> backgroundModel, double lambdaB) {
    for (String word : this.probabilitiesHiddenVariablesBackgroundModel.keySet()) {
      Fraction numerator = backgroundModel.get(word).multiply(new Fraction(lambdaB));
      Fraction temp = Fraction.ZERO;
      for (Theme otherTheme : probabilitiesDocumentBelongsToThemes.keySet()) {
        temp.add(this.probabilitiesDocumentBelongsToThemes.get(otherTheme).multiply(otherTheme.wordsProbability.get(word)));
      }
      Fraction denominator = numerator.add((Fraction.ONE.subtract(new Fraction(lambdaB)).multiply(temp)));
      
      this.probabilitiesHiddenVariablesBackgroundModel.put(word, numerator.divide(denominator));
    }
  }
  
  /**
   * Update probabilities document belongs to themes
   */
  
  public Fraction subUpdateProbabilitiesDocumentBelongsToThemes(Theme theme) {
    Fraction value = Fraction.ZERO;
    for (String word : this.words.keySet()) {
      value.add(new Fraction(this.words.get(word)).multiply(
              Fraction.ONE.subtract(this.probabilitiesHiddenVariablesBackgroundModel.get(word))).multiply(
                      this.probabilitiesHiddenVariablesThemes.get(Pair.of(word, theme))));
    }
    return value;
  }
  
  public void updateProbabilitiesDocumentBelongsToThemes() {
    Fraction denominator = Fraction.ZERO;
    for (Theme theme : this.probabilitiesDocumentBelongsToThemes.keySet()) {
      denominator.add(subUpdateProbabilitiesDocumentBelongsToThemes(theme));  
    }
    
    for (Theme theme : this.probabilitiesDocumentBelongsToThemes.keySet()) {
      Fraction numerator = subUpdateProbabilitiesDocumentBelongsToThemes(theme);
      this.probabilitiesDocumentBelongsToThemes.put(theme, numerator.divide(denominator));
    }
  }
}
