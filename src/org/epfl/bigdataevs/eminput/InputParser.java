package org.epfl.bigdataevs.eminput;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Time;
import java.text.ParseException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;


/**Team: Matias and Christian.
*InputParser: parses the data read from HDFS, clean them, 
*and computes the background model for the EM algorithm
**/
public class InputParser {

  
  private final JavaSparkContext sparkContext;
  private final JavaRDD<SegmentedArticle> segmentedArticles;
  private final TimePeriod timeFrame;
  private final BackgroundModel backgroundModel;
  
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
    
    JavaRDD<RawArticle> rawArticles = getRawArticleRDD(timeFrame, sourceList, config);
    segmentedArticles = rawArticles.map(new SegmentArticle());
    
    backgroundModel = new BackgroundModel(segmentedArticles);
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
  public HmmInputFromParser getHmmInput(Time dt) {
    
    return new HmmInputFromParser(backgroundModel, segmentedArticles, timeFrame, dt);
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


/** Acts as an intermediary processing step between a RawArticle and a ParsedArticle.
 * The SegmentedArticle contains the ordered list of words constituting the original
 * article text. It also has a list of TimePeriods it belongs to. **/
class SegmentedArticle implements Serializable {
  
  public final List<String> words;
  public final ArticleStream stream;
  public final Date publication;
  
  protected SegmentedArticle( List<String> words, ArticleStream stream, 
          Date publication) {
    this.words = words;
    this.stream = stream;
    this.publication = publication;
  }
}

@SuppressWarnings("serial")
class SegmentArticle implements Function<RawArticle, SegmentedArticle>, Serializable {

  /** Article texts are split on anything not a letter or number. **/
  public static final String WORD_SPLIT_PATTERN = "[^\\p{L}\\p{Nd}]+";
  
  /** Splits the RawArticle's text into a list of words; turn result into a SegmentedArticle.
   * instance **/
  public SegmentedArticle call(RawArticle article) {
    
    String[] words = article.fullText.split(WORD_SPLIT_PATTERN);
    LinkedList<String> cleanedWords = new LinkedList<String>();
    for (String word : words) {
      cleanedWords.add(word.toLowerCase());
    }
    return new SegmentedArticle(cleanedWords, article.stream, 
            article.issueDate);
  }
}