package org.epfl.bigdataevs.em;

import org.apache.commons.math3.fraction.BigFraction;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePeriod;

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
import java.util.Random;
import java.util.TreeMap;

public class Theme extends TimePeriod{
    public Map<String, Double> wordsProbability;
    public final static int RANDOM_MAX = 1000;
    public Long partitionIndex = 0L;
    
    public Theme(Date from, Date to){
      super(from, to);
      this.wordsProbability = new HashMap<>();
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
            int numerator = random.nextInt(RANDOM_MAX)+1;
            numerators.add(numerator);
            total += numerator;
          }
        }
      }
      
      
      for (int i = 0; i < wordsOfPartitions.size(); i++) {
        Double value = new Double(numerators.get(i)) / total;
        this.wordsProbability.put(wordsOfPartitions.get(i), value);
      }
    }
    
    
    public String toString() {
      String s = "Theme #"+this.partitionIndex;
      for (String word : this.wordsProbability.keySet()) {
        s += word+" : "+this.wordsProbability.get(word);
      }
      return s;
    }
    
    public TreeMap<String, Double> sortString() {
      TreeMap<String, Double> sortedMap = new TreeMap<String, Double>(new ValueComparator(wordsProbability));
      sortedMap.putAll(wordsProbability);
      return sortedMap;
    }
    
    
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
}
