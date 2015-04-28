package org.epfl.bigdataevs.eminput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.sql.Time;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.ErrorManager;

import javax.xml.stream.XMLStreamException;


/**Team: Matias and Christian.
*InputParser: parses the data read from HDFS, clean them, 
*and computes the background model for the EM algorithm
**/
public class InputParser {
 
  private final JavaSparkContext sparkContext;
  private final JavaRDD<RawArticle> rawArticles;
  private final TimePeriod timeFrame;
  
  /**Initialize a parser on the dataset. EM and HMM can get their input form there.
   * @param timeFrame The TimePeriod for which all articles will be loaded 
   * @param sparkContext the Spark Context
   * @param sourcePath the full path to the data (HDFS or local)
   * @throws NumberFormatException
   * @throws XMLStreamException
   * @throws ParseException
   * @throws IOException
   */
  public InputParser(TimePeriod timeFrame,
          JavaSparkContext sparkContext,
          List<String> sourcePath) 
                  throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    this.timeFrame = timeFrame;
    this.sparkContext = sparkContext;
    
    Configuration config = new Configuration();
    
    List<String> sourceList = new LinkedList<String>();
    sourceList.addAll(sourcePath);
    
    rawArticles = getRawArticleRDD(timeFrame, sourceList, config);  
  }
  
  /** Returns the input for the EMAlgorithm.
    * @param partitioning a partitioning over the timeFrame. All timePeriod in the
    *   list should be included in the timeFrame used to construct the InputParser.
    * @return container for the background model and word
    *     count of every individual article in each stream.
   **/
  public TextCollectionData getEmInput(List<TimePeriod> partitioning) {
    
    for(TimePeriod tp : partitioning) {
      if (this.timeFrame.contains(tp)) {
        throw new IllegalArgumentException("Partition TimePeriod not contained in the timeFrame of this Parser");
      }
    }    
    
    return null;
  }
  
  /**Returns the input for the HMM Algorithm.
   * @param dt the time interval to be used for timestamps frequency.
   * @return the input of the HMM Algo.
   */
  public TextCollectionData getHmmInput(Time dt) {
    
    return null;
  }
  
  // TODO: Do this out of master node
   private JavaRDD<RawArticle> getRawArticleRDD(TimePeriod englobingTimePeriod,
           List<String> sourceList,
           Configuration config) 
                   throws NumberFormatException, XMLStreamException, ParseException, IOException {
     
     RawArticleInputStream ras = new RawArticleInputStream(englobingTimePeriod, sourceList, config);
     
     RawArticle rawArticle;
     List<RawArticle> rawArticleList = new LinkedList<RawArticle>();
     
     while ((rawArticle = ras.read()) != null) {
       rawArticleList.add(rawArticle);
     }
     
     return sparkContext.parallelize(rawArticleList); 
   }
}