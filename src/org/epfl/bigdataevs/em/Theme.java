package org.epfl.bigdataevs.em;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.epfl.bigdataevs.eminput.EmInput;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePeriod;

public class Theme extends TimePeriod{
    public JavaPairRDD<String, Double> wordsProbability;
	
    public Theme( Date from, Date to){
      super(from, to);
    }
    
    
    //initiate theme probabilities
    @SuppressWarnings("unchecked")
    public void initialization(EmInput input) {
    
      JavaRDD wordsOfPartition = input.parsedArticles.flatMapValues(new Function<ParsedArticle, Iterable<String>>() {
        @Override
        public Iterable<String> call(ParsedArticle article) throws Exception {
          return (Iterable<String>) article.words.keys().distinct();
        }
      }).values().distinct();
      
      long numberOfElement = wordsOfPartition.count();
      this.wordsProbability = wordsOfPartition.mapToPair(new PairFunction<String, String, Fraction>() {
        
        @Override
        public Tuple2<String, Fraction> call(String word) throws Exception {
          // TODO Auto-generated method stub
          return new Tuple2<String, Fraction>(word, new Fraction(1, numberOfElement));
        }
      });
    }
    
    public double divergence(Theme t){
      double result = 0.;
      
      for(String word : t.wordsProbability.keySet()){
        if(wordsProbability.containsKey(word)){
          double p1 = t.wordsProbability.get(word);
          double p2 = wordsProbability.get(word);
          
          if(p1 > 0.f){
            result += p2*Math.log(p2/p1);
          }
        }
      }
      
      return(result);
    }
    
}
