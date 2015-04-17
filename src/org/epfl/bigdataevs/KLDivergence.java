package org.epfl.bigdataevs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.epfl.bigdataevs.em.Theme;

import scala.Tuple2;

import java.util.LinkedList;
import java.util.Set;

public class KLDivergence {

  public class EvolutionaryTransition{
    public Theme theme1;
    public Theme theme2;
    public double divergence;
    
    /**
     * @author antoinexp & lfaucon
     * 
     * @param t1 The first theme (chronological order)
     * @param t2 The second theme (chronological order)
     * @param divergence The Kullback divergence D(t1||t2). It shows the strength of the link
     *     between theme1 and theme2
     */
    public EvolutionaryTransition(Theme t1, Theme t2, double divergence) {
      this.theme1 = t1;
      this.theme2 = t2;
      this.divergence = divergence;
    }
  }

  private static double threshold;
  
  KLDivergence(double threshold) {
    KLDivergence.threshold = threshold;
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
        if (divergence > 0) {
          LinkedList<EvolutionaryTransition> evolutionaryTransition = 
                  new LinkedList<EvolutionaryTransition>();
          evolutionaryTransition.add(new EvolutionaryTransition(theme1,theme2,divergence));
          return evolutionaryTransition;
        } else {
          return new LinkedList<EvolutionaryTransition>();
        }
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
  private static double transitionDistance(Theme t1,Theme t2) {
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
  public static double divergence(Theme t1, Theme t2) {
    //This variable is used to smooth the probability distribution
    double epsilon = 0.0001d;
    
    double result = 0.;
    Set<String> set = t1.wordsProbability.keySet();
    int numberOfWords = set.size();
    for (String word : set) {
      if (t2.wordsProbability.containsKey(word)) {
        double p1 = t1.wordsProbability.get(word).doubleValue();
        double p2 = t2.wordsProbability.get(word).doubleValue();
        //smoothing
        p1 = (p1 + epsilon) / (1. + numberOfWords * epsilon);
        result += p2 * Math.log(p2 / p1);
      }
    }
    
    return result;
  }
}
