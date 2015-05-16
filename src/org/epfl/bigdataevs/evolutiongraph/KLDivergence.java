package org.epfl.bigdataevs.evolutiongraph;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.epfl.bigdataevs.em.LightTheme;
import org.epfl.bigdataevs.executables.Parameters;

import scala.Tuple2;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Set;

public class KLDivergence implements Serializable{

  private double threshold;
  private int numPartitions;
  //This variable is an upperbound to avoid the NaN when computing log(p/0)
  private Double logMax;
  private Double epsilon;
  
  private boolean totalVariationDistance = false;
  
  public KLDivergence(double threshold, double logMax, int numPartitions) {
    this.threshold = threshold;
    this.logMax = logMax;
    this.numPartitions = numPartitions;
    totalVariationDistance = Parameters.totalVariationDistance;
    epsilon = 1e-7;
  }
  
  /**
   * @author antoinexp & lfaucon
   * 
   * @param themes A JavaRDD of all the extracted themes
   * @return A JavaRDD of all the Evolutionary Transition
   */
  
  public JavaRDD<EvolutionaryTransition> compute(final JavaRDD<LightTheme> themes) {
    JavaPairRDD<LightTheme, LightTheme> pairs;
    JavaRDD<LightTheme> themesPartitionned = themes.repartition(numPartitions);
    pairs = themesPartitionned.cartesian(themesPartitionned);

    return pairs.flatMap(new FlatMapFunction<Tuple2<LightTheme,LightTheme>, EvolutionaryTransition>(){

      @Override
      public Iterable<EvolutionaryTransition> call(Tuple2<LightTheme, LightTheme> theme) 
              throws Exception {        
        LightTheme theme1 = theme._1();
        LightTheme theme2 = theme._2();
        double divergence;
        
        if (totalVariationDistance) {
          divergence = totalVariation(theme2,theme1);
        } else {
          divergence = divergence(theme2,theme1);
        }
        
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
  
  private double transitionDistance(LightTheme t1,LightTheme t2) {
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
  
  public double divergence(LightTheme t1, LightTheme t2) {
    Double result = new Double(0.);
    Set<String> set = t2.wordsProbability.keySet();
    int n = set.size();

    for (String word : set) {
      double p2 = t2.wordsProbability.get(word);
      double p1 = 0.;
      
      if (t1.wordsProbability.containsKey(word)) {
        p1 = t1.wordsProbability.get(word);
      }
      p1 = p1 + epsilon;
      p2 = p2 + epsilon;
      
      //smoothing
      result += p2 * smoothLog(p1,p2);
    }
    result /= 1 + n * epsilon;
    return result;
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
  public double totalVariation(LightTheme t1, LightTheme t2) {
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
    Double result = new Double(Math.min(logMax,Math.max(-logMax,Math.log(p2 / p1))));
    if (result.isNaN()) {
      return logMax;
    } else {
      return result;
    }
  }
  
  
}
