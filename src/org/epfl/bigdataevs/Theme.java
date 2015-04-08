package org.epfl.bigdataevs;

import java.io.Serializable;
import java.util.Map;

import org.epfl.bigdataevs.eminput.TimePeriod;


public class Theme {

    public Map<String, Double> wordsProbability;
    public TimePeriod timeSpan;
	
    public Theme(Map<String, Double> wordsProbability, TimePeriod timeSpan){
      this.wordsProbability = wordsProbability;
      this.timeSpan = timeSpan;
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
