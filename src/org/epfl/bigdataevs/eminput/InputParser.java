package org.epfl.bigdataevs.eminput;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;


/**Team: Matias and Christian.
*InputParser: parses the data read from HDFS, clean them, 
*and computes the background model for the EM algorithm
**/
public class InputParser {
 
  /** Parses data for all streams, cleans it and returns
    * an EmInput instance containing all the desired content
    * (background model and word distribution for every stream) for
    * the given time period (or frame).
    * @timePeriod the time interval that will be considered 
    *     for the streams.
    * @return container for the background model and word 
    *     count of every individual article in each stream**/
  public JavaPairRDD<TimePeriod,TimePartition> getEmInput(List<TimePeriod> timePeriod) {
    
    if (timePeriod == null) {
      return null; 
    }
    return null;
  }
}