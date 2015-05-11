package org.epfl.bigdataevs.evolutiongraph;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.epfl.bigdataevs.em.Theme;

import scala.Tuple2;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Set;

public class KLDivergence implements Serializable{

  private double threshold;
  private int numPartitions;
  //This variable is an upperbound to avoid the NaN when computing log(p/0)
  private Double logMax;
  
  public KLDivergence(double threshold, double logMax, int numPartitions) {
    this.threshold = threshold;
    this.logMax = logMax;
    this.numPartitions = numPartitions;
  }
  
  /**
   * @author antoinexp & lfaucon
   * 
   * @param themes A JavaRDD of all the extracted themes
   * @return A JavaRDD of all the Evolutionary Transition
   */
  
  public JavaRDD<EvolutionaryTransition> compute(final JavaRDD<Theme> themes) {
    JavaPairRDD<Theme, Theme> pairs;
    pairs = themes.cartesian(themes);
    pairs.repartition(numPartitions);

    return pairs.flatMap(new FlatMapFunction<Tuple2<Theme,Theme>, EvolutionaryTransition>(){

      @Override
      public Iterable<EvolutionaryTransition> call(Tuple2<Theme, Theme> theme) 
              throws Exception {        
        Theme theme1 = theme._1();
        Theme theme2 = theme._2();
        double divergence = divergence(theme2,theme1);
        
        LinkedList<EvolutionaryTransition> evolutionaryTransition = 
                new LinkedList<EvolutionaryTransition>();
        
        if (theme1.lessThan(theme2)) {
          if (divergence < threshold) {
            evolutionaryTransition.add(new EvolutionaryTransition(theme1,theme2,divergence));
          }
        }
        return evolutionaryTransition;
      }
    });
    
  }
  
  /**
   * @author antoinexp & lfaucon
   * 
   * @param t1 A theme
   * @param t2 An other theme
   * @return returns the divergence(>0 always) if t1 and t2 form an Evolutionary Transition 
   *     and -1 otherwise
   */
  
  private double transitionDistance(Theme t1,Theme t2) {
    if (t1.lessThan(t2)) {
      double divergence = divergence(t2,t1);
      if (divergence < threshold) {
        return divergence;
      } else {
        return -1d;
      }
    } else {
      return -1d;
    }
  }
  
  /**
   * @author antoinexp & lfaucon
   * 
   * @param t1 A theme
   * @param t2 An other theme
   * @return returns the Kullback divergence D(t1||t2)
   */
  
  public double divergence(Theme t1, Theme t2) {
    Double result = new Double(0.);
    Set<String> set = t2.wordsProbability.keySet();
    
    for (String word : set) {
      double p2 = t2.wordsProbability.get(word);
      double p1 = 0.;
      
      if (t1.wordsProbability.containsKey(word)) {
        p1 = t1.wordsProbability.get(word);
      }
      
      //smoothing
      result += p2 * smoothLog(p1,p2);
    }
    return result.isNaN() ? 42 : result;
  }
  
  
  /**
   * @author lfaucon
   * 
   * This Total Variation is an alternative metric that can be used instead of Kullback divergence
   * 
   * @param t1 A theme 
   * @param t2 A theme
   * @return The total variation between the two probability distributions
   */
  public double totalVariation(Theme t1, Theme t2) {
    double result = 0.;
    for (String word : t2.wordsProbability.keySet()) {
      double p2 = t2.wordsProbability.get(word);
      double p1 = 0.;
      
      if (t1.wordsProbability.containsKey(word)) {
        p1 = t1.wordsProbability.get(word);
      }
      
      if (p2 > p1) {
        result += p2 - p1;
      }
    }
    return result;
  }
  
  /**
   * @author lfaucon
   * 
   * @param p1 a probability
   * @param p2 a probability
   * @return log(p2/p1)
   */
  public Double smoothLog(double p1,double p2){
    Double result = new Double(Math.min(100,Math.abs(Math.log(p2 / p1))));
    if (result.isNaN()) {
      return logMax;
    } else {
      return result;
    }
  }
  
  
}
