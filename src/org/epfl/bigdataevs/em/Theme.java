package org.epfl.bigdataevs.em;

import org.apache.commons.math3.fraction.Fraction;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePeriod;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

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
    
}
