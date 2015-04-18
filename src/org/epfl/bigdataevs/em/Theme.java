package org.epfl.bigdataevs.em;

import org.apache.commons.math3.fraction.Fraction;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePeriod;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

public class Theme extends TimePeriod{
  public HashMap<String, Fraction> wordsProbability;
  public final static int RANDOM_MAX = 100;
  public Long partitionIndex = 0L;
  public Long id = 0L; // This variable is used for the output
  
  public Theme(Date from, Date to){
    super(from, to);
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
    int total = 0;
    
    for(ParsedArticle article: input.parsedArticles) {
      for(String word: article.words.keySet()) {
        if(!wordsOfPartitions.contains(word)) {
          wordsOfPartitions.add(word);
          int numerator = random.nextInt(RANDOM_MAX);
          numerators.add(numerator);
          total += numerator;
        }
      }
    }
    
    for (int i = 0; i < wordsOfPartitions.size(); i++) {
      this.wordsProbability.put(wordsOfPartitions.get(i), new Fraction(numerators.get(i), total));
    }
  }
  
  
  /**
   * Extract the k most relevant words associated with the theme
   * 
   * @author antoinexp & lfaucon
   * 
   * @param k the number of words
   * @return returns a string list containing the k most frequent words
   */
  public List<String> themeFeatures(int k) {
    List<String> list = new LinkedList<String>();
    TreeMap<Double, String> sortedMap = new TreeMap<Double, String>();
    int i = 0;
    
    for (Entry<String, Fraction> entry : wordsProbability.entrySet()) {
        sortedMap.put(entry.getValue().doubleValue(), entry.getKey());
    }
    
    for (i=0; i<k; i++) {
      Entry<Double, String> entry = sortedMap.pollLastEntry();
      list.add(entry.getValue());
    }
    
    return(list);
  }
}
