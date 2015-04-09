package org.epfl.bigdataevs.em;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.epfl.bigdataevs.*;
import org.epfl.bigdataevs.eminput.EmInput;

import java.util.ArrayList;
import java.util.LinkedList;

public class EmAlgo {
  public JavaRDD<EmInput> partitions;
  public int numberOfThemes;
  public double lambdaBackgroundModel;

  
  public EmAlgo(JavaRDD<EmInput> partitions) {
    this.partitions = partitions;
  }
  
  public void initialization() {
    // Initialize probabilities to all documents
    
  }
  
  public JavaRDD<Theme> algo() {
    // Creation of RDD
    LinkedList<Theme> themesList = new LinkedList<Theme>();
    
    
  }
  
  
  
}
