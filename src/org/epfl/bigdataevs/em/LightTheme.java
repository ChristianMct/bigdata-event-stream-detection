package org.epfl.bigdataevs.em;

import org.epfl.bigdataevs.em.Theme.ValueComparator;
import org.epfl.bigdataevs.input.TimePeriod;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


public class LightTheme implements Serializable {
  public TimePeriod timePeriod;
  public Map<String, Double> wordsProbability;
  
  
  public LightTheme(Theme theme){
    timePeriod = new TimePeriod(theme.timePeriod);
    wordsProbability = new HashMap<>(theme.wordsProbability);
  }
  
  public boolean lessThan(LightTheme other) {
    return timePeriod.lessThan(other.timePeriod);
  }
  
  
  public TreeMap<String, Double> sortString(int maxWords, int minChar) {
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
      if (word.length() > minChar) {
        smallSortedMap2.put(word, this.wordsProbability.get(word));
        count += 1;
      }
    }
    return smallSortedMap2;
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
  
  
  public int hashCode() {
    return wordsProbability.hashCode();
  }
  
  public String toString(){
    TreeMap<String, Double> treeMap = sortString(3, 5);
    String result = new String();
    
    for(String word : treeMap.keySet()){
      result += word+" ";
    }
    
    return result;
  }
  
}


