package org.epfl.bigdataevs;

import org.apache.spark.api.java.JavaRDD;

public class EmAlgo {
  public JavaRDD<EmInput> partitions;
  public int numberOfThemes;
  public double lambdaBackgroundModel;

  
  public EMAlgo(JavaRDD<EmInput> partitions) {
    this.partitions = partitions;
  }
  
  public void initialization() {
    // initialize probabilities
  }
  
  public JavaRDD<Theme> algo() {
    
  }
  
}
