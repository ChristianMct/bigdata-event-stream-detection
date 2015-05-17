package org.epfl.bigdataevs.em;

import org.apache.commons.math3.fraction.BigFraction;
import org.epfl.bigdataevs.eminput.TimePeriod;

import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

/**
 * Team : Nina & Antoine
 * 
 * Representation of a theme
 * Contain a probabilistic distribution of word, 
 * a TimePeriod and articles sorted by their belonging to this theme
 * 
 * @author abastien
 *
 */

public class Theme implements Serializable {
  public Map<String, Double> wordsProbability;
  public final static int RANDOM_MAX = 1000;
  public Long partitionIndex = 0L;
  public int index;
  public TimePeriod timePeriod;
  public List<Document> sortedArticlesByScore;
  
  public Theme(Date from, Date to, int index) {
    this.timePeriod = new TimePeriod(from, to);
    this.wordsProbability = new HashMap<>();
    this.index = index;
  }
    
    /**
     * Initialize the probabilities that describes a theme
     * At the beginning, the probabilities are randomly distributed
     * @param Eminput (partition)
     */
  public void initialization(EmInput input) {
    ArrayList<String> wordsOfPartitions = new ArrayList<>();
    ArrayList<Integer> numerators = new ArrayList<>();
    Random random = new Random();
    double total = 0.0;
    
    for (Document article: input.documents) {
      for (String word: article.words.keySet()) {
        if (!wordsOfPartitions.contains(word)) {
          wordsOfPartitions.add(word);
          int numerator = random.nextInt(RANDOM_MAX) + 1;
          numerators.add(numerator);
          total += (double) numerator;
        }
      } 
    }
    
    for (int i = 0; i < wordsOfPartitions.size(); i++) {
      double value = numerators.get(i) / total;
      this.wordsProbability.put(wordsOfPartitions.get(i), value);
    }
  }
  
  /**
   * Sort the map of words describing a theme by its probabilities (value)
   * @param maxWords
   * @return the first maxWords words describing a theme with their probabilitites
   */
  public TreeMap<String, Double> sortString(int maxWords) {
    TreeMap<String, Double> smallSortedMap = new TreeMap<String, Double>(new ValueComparator(wordsProbability));
    TreeMap<String, Double> smallSortedMap2 = new TreeMap<String, Double>(new ValueComparator(wordsProbability));
    int count = 0;
    for (String word : this.wordsProbability.keySet()) {
        smallSortedMap.put(word, this.wordsProbability.get(word));
    }
    for (String word : smallSortedMap.keySet()) {
      if (count > maxWords) {
        break;
      }
      if (word.length() >= 4) {
        smallSortedMap2.put(word, this.wordsProbability.get(word));
        count += 1;
      }
    }
    return smallSortedMap2;
  }
    
  /**
   * Comparator used to sort maps by their double values
   * @author abastien
   *
   */
  class ValueComparator implements Comparator<String> {

    Map<String, Double> base;
    
    public ValueComparator(Map<String, Double> base) {
      this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b) {
      if (base.get(a).compareTo(base.get(b)) == 1) {
        return -1;
      } else {
        return 1;
      } // returning 0 would merge keys
    }
  }
  
  /**
   * Get the titles of the articles that have the highest probability to belong to this theme
   * @return a list of articles
   */
  public String sortTitleString(int maxTitles) {
    String output = "";
    for (int i = 0; i < Math.min(maxTitles, this.sortedArticlesByScore.size()); i++) {
      String title = this.sortedArticlesByScore.get(i).title;
      Double score = this.sortedArticlesByScore.get(i).probabilitiesDocumentBelongsToThemes.get(this);
      output += "\t" + title + " : " + score + "\n";
    }
    return output;
  }
  
  /**
   * @author lfaucon & abastien
   * 
   * @param other : an other theme
   * @return true when other corresponds to a time period strictly after this.timeperiod
   */
  public boolean lessThan(Theme other) {
    return timePeriod.lessThan(other.timePeriod);
  }

  
  
  /**
   * Extract the k most relevant words associated with the theme
   * 
   * @author antoinexp & lfaucon
   * 
   * @param k the number of words
   * @return returns a string list containing the k most frequent words
   */
  /*public List<String> themeFeatures(int k) {
    List<String> list = new LinkedList<String>();
    TreeMap<Double, String> sortedMap = new TreeMap<Double, String>();
    int i = 0;
    
    for (Entry<String, Double> entry : wordsProbability.entrySet()) {
        sortedMap.put(entry.getValue().doubleValue(), entry.getKey());
    }
    
    for (i=0; i<k; i++) {
      Entry<Double, String> entry = sortedMap.pollLastEntry();
      list.add(entry.getValue());
    }
    
    return(list);
  }*/
  
  /**
   * Extract the k most relevant words associated with the theme
   * 
   * @author antoinexp & lfaucon
   * 
   * @param k the number of words
   * @return returns a string list containing the k most frequent words
   */
  public Map<String, Double> themeFeatures(Map<String, Double> backgroundModel, double epsilon, int k) {
    Map<String, Double> result = new HashMap<String, Double>();
    double maxScore = 0.;
    String bestWord = null;
    Set<String> set = this.wordsProbability.keySet();
    int numberOfWords = backgroundModel.size();
    
    for(int i=0; i<k; i++){
      maxScore = 0.;
      bestWord = "NOWORDFOUND";
      
      for (String word : set) {
        double p2 = this.wordsProbability.get(word).doubleValue();
        double p1 = 0.;
        
        if (backgroundModel.containsKey(word)) {
          p1 = backgroundModel.get(word).doubleValue();
        }
        
        //smoothing
        p1 = (p1 + epsilon) / (1. + numberOfWords * epsilon);
        
        double score = p2 * Math.log(p2 / p1);
        if(score > maxScore && !result.containsKey(word)){
          maxScore = score;
          bestWord = word;
        }
      }
      
      result.put(bestWord, maxScore);
    }
      
    return(result);
  }
}
