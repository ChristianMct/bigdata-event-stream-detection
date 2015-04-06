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
    
  public static float compute(Theme theme1, Theme theme2){
    return 0.f;
  }
    
  public static JavaPairRDD<Float, ThemeCouple> compute(JavaRDD<Theme> theme_partition1, JavaRDD<Theme> theme_partition2){
    return null;
  }
}
