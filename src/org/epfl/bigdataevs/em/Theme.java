package org.epfl.bigdataevs.em;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import scala.Tuple2;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.epfl.bigdataevs.eminput.EmInput;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePeriod;

public class Theme extends TimePeriod{
    public HashMap<String, Fraction> wordsProbability;
    public final static int RANDOM_MAX = 100;
	
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
    
    public double divergence(Theme t){
      double result = 0.;
      
      for(String word : t.wordsProbability.keySet()){
        if(wordsProbability.containsKey(word)){
          Fraction p1 = t.wordsProbability.get(word);
          Fraction p2 = wordsProbability.get(word);
          
          if(p1 > 0.f){
            result += p2*Math.log(p2/p1);
          }
        }
      }
      
      return(result);
    }
    
}
