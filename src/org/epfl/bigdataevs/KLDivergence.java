package org.epfl.bigdataevs;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.epfl.bigdataevs.em.Theme;

import scala.Tuple2;


public class KLDivergence {

  public class ThemeCouple{
    public Theme a, b;
    public double divergence;
    
    public ThemeCouple(Theme a, Theme b){
	    this.a = a;
	    this.b = b;
    }
  }

  private double threshold;
  
  KLDivergence(double threshold){
    this.threshold = threshold;
  }
    
  /**
   * @author antoinexp & lfaucon
   * 
   * @param themes A JavaRDD of all the extracted themes
   * @return A JavaRDD of all the Evolutionary Transition
   */
  public JavaRDD<ThemeCouple> compute(final JavaRDD<Theme> themes) {
    JavaPairRDD<Theme, Theme> pairs;
    
    pairs = themes.cartesian(themes);
    pairs.flatMap(new FlatMapFunction<Tuple2<Theme,Theme>, ThemeCouple>(){

      @Override
      public Iterable<ThemeCouple> call(Tuple2<Theme, Theme> theme) throws Exception {        
        Theme theme1 = theme._1();
        Theme theme2 = theme._2();
        
        if(theme1.lessThan(theme2)) {
          /* TODO */
        }
        
        return null;
      }
      
      
    });
    
    return null;
  }
  
  /**
   * @author antoinexp & lfaucon
   * 
   * @param t1 A theme
   * @param t2 An other them
   * @return returns the Kullback divergence D(t1||t2)
   */
  public double divergence(Theme t1, Theme t2){
    double result = 0.;
    
    for(String word : t1.wordsProbability.keySet()){
      if(t2.wordsProbability.containsKey(word)){
        double p1 = t1.wordsProbability.get(word).doubleValue();
        double p2 = t2.wordsProbability.get(word).doubleValue();
        
        if(p1 > 0.f){
          result += p2*Math.log(p2/p1);
        }
      }
    }
    
    return(result);
  }
}
