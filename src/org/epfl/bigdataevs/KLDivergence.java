package org.epfl.bigdataevs;

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
    
    
  public JavaRDD<ThemeCouple> compute(final JavaRDD<Theme> themes){
    JavaPairRDD<Theme, Theme> pairs;
    
    pairs = themes.cartesian(themes);
    pairs.flatMap(new FlatMapFunction<Tuple2<Theme,Theme>, ThemeCouple>(){

      @Override
      public Iterable<ThemeCouple> call(Tuple2<Theme, Theme> theme) throws Exception {        
        Theme theme1 = theme._1();
        Theme theme2 = theme._2();
        
        if(theme1.lessThan(theme2)){
          /* TODO */
        }
        
        return null;
      }
      
      
    });
    
    return null;
  }
}
