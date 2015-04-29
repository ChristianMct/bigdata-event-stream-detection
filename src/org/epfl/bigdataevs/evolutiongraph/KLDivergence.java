package org.epfl.bigdataevs.evolutiongraph;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.epfl.bigdataevs.em.Theme;

import scala.Tuple2;

import java.util.LinkedList;
import java.util.Set;

public class KLDivergence {

  private double threshold;
  private double epsilon; //This variable is used to smooth the probability distribution
  
  public KLDivergence(double threshold, double epsilon) {
    this.threshold = threshold;
    this.epsilon = epsilon;
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
    pairs.flatMap(new FlatMapFunction<Tuple2<Theme,Theme>, EvolutionaryTransition>(){

      @Override
      public Iterable<EvolutionaryTransition> call(Tuple2<Theme, Theme> theme) 
              throws Exception {        
        Theme theme1 = theme._1();
        Theme theme2 = theme._2();
        double divergence = transitionDistance(theme1,theme2);
        LinkedList<EvolutionaryTransition> evolutionaryTransition = 
                new LinkedList<EvolutionaryTransition>();
        
        if (divergence > 0) {
          evolutionaryTransition.add(new EvolutionaryTransition(theme1,theme2,divergence));
        }
        
        return evolutionaryTransition;
      }
    });
    
    return null;
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
    double result = 0.;
    Set<String> set = t2.wordsProbability.keySet();
    int numberOfWords = t1.wordsProbability.size();
    
    for (String word : set) {
      double p2 = t2.wordsProbability.get(word).doubleValue();
      double p1 = 0.;
      
      if (t1.wordsProbability.containsKey(word)) {
        p1 = t1.wordsProbability.get(word).doubleValue();
      }
      
      //smoothing
      p1 = (p1 + epsilon) / (1. + numberOfWords * epsilon);
      result += p2 * Math.log(p2 / p1);
    }
    
    return result;
  }
  
}
