package org.epfl.bigdataevs.em;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.epfl.bigdataevs.eminput.EmInput;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePeriod;


public class Theme extends TimePeriod{
    public Map<String, Double> wordsProbability;
	
    public Theme( Date from, Date to){
      super(from, to);
      this.wordsProbability = new HashMap<String, Double>();
    }
    
    
    //initiate theme probabilities
    public void initialization(EmInput input) {
    
      input.parsedArticles.flatMap(new FlatMapFunction2<Date, ParsedArticle, >() {
        @Override
        public Iterable<ParsedArticle> call(Date date) throws Exception {
          // TODO Auto-generated method stub
          return null;
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
