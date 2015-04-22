package org.epfl.bigdataevs.em;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.fraction.BigFraction;
import org.epfl.bigdataevs.eminput.TimePartition;
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
  /** Map containing tuples of words and their 
   * distribution in the streams. **/
  public Map<String, Double> backgroundModel;
  /** Collection containing articles published at that time. **/
  public Collection<Document> Documents;
  
  /** List of the themes appearing in this input*/
  public ArrayList<Theme> themesOfPartition;
  
  /** Index of the EmInput in the RDD**/
  public Long indexOfPartition = 0L;
  
  /** Index of the run for this EmInput**/
  public int run;
  
  /** Time period containing all articles**/
  public TimePeriod timePeriod;
  
  /**
   * EmInput contains at least the background model, 
   * the list of articles and the period delimiting these articles.
   * @param backgroundModel
   * @param Documents
   * @param period
   */
  public EmInput(Map<String, Double> backgroundModel,
          Collection<Document> Documents, TimePeriod period) {
    
    this.backgroundModel = backgroundModel;
    this.Documents = Documents;
    this.timePeriod = period;
    this.themesOfPartition = new ArrayList<>();
  }
  
  /**
   * EmInput builds with an instance of TimePartition
   * @param backgroundModel
   * @param Documents
   * @param period
   */
  /*
  public EmInput(TimePartition timePartition) {
    this.backgroundModel = timePartition.backgroundModel;
    this.Documents = timePartition.Documents;
    this.timePeriod = timePartition.timePeriod;
    this.themesOfPartition = new ArrayList<>();
  }
  */
  
  public void addTheme(Theme theme) {
    this.themesOfPartition.add(theme);
  }
  
  /**
   * Initialize all probabilities in the articles (article d belongs to theme j)
   */
  public void initializeArticlesProbabilities() {
    for (Document article : this.Documents) {
      article.initializeProbabilities(themesOfPartition);
    }
  }
  
  /**
   * Compute the log-likelihood of the mixture model
   * @return log-likelohood
   */
  public double computeLogLikelihood(double lambdaBackgroundModel) {
    double logLikelihood = 0.0;
    for (Document Document : Documents) {
      for (String word : Document.words.keySet()) {
        double temp = 0.0;
        for (Theme theme : themesOfPartition) {
          temp = temp + (Document.probabilitiesDocumentBelongsToThemes.get(theme)
                  * theme.wordsProbability.get(word));
        }
        logLikelihood += Document.words.get(word) * Math.log(
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
    for (Document Document : Documents) {
      if (Document.words.containsKey(word)) {
        value = value + (((double) Document.words.get(word))
                * (1.0 - Document.probabilitiesHiddenVariablesBackgroundModel.get(word)) 
                        * (Document.probabilitiesHiddenVariablesThemes.get(
                                Pair.of(word, theme))));
      }
    }
    return value;
  }
  
  /**
   * Update the probabilities that words belongs to themes
   * Do the computation for every themes
   */
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
  
  /**
   * Clone the EmInput by replacing every article.
   */
  public EmInput clone() {
    Collection<Document> articles = new ArrayList<>();
    for (Document Document : this.Documents) {
      articles.add(new Document(Document.words, Document.stream));
    }
    return new EmInput(this.backgroundModel, articles, this.timePeriod);
  }
}
