package org.epfl.bigdataevs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;


public class KLDivergence {

  public class ThemeCouple{
    public Theme a, b;
    
    public ThemeCouple(Theme a, Theme b){
	    this.a = a;
	    this.b = b;
    }
  }

  private double threshold;
  
  KLDivergence(double threshold){
    this.threshold = threshold;
  }
    
    
  public static JavaPairRDD<Double, ThemeCouple> compute(JavaRDD<Theme> theme_partition1, JavaRDD<Theme> theme_partition2){

    
    return null;
  }
}
