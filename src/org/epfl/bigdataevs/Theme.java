package org.epfl.bigdataevs;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.epfl.bigdataevs.eminput.TimePeriod;


public class Theme extends TimePeriod{
    public Map<String, Double> wordsProbability;
	
    public Theme( Date from, Date to){
      super(from, to);
      this.wordsProbability = new HashMap<String, Double>();
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
