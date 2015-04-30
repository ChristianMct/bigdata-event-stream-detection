package org.epfl.bigdataevs.eminput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;


/**Team: Matias and Christian.
*InputParser: parses the data read from HDFS, clean them, 
*and computes the background model for the EM algorithm
**/
public class InputParser implements Serializable {
  private final JavaRDD<SegmentedArticle> segmentedArticles;
  private final TimePeriod timeFrame;
  private final BackgroundModel backgroundModel;
  
  /**Initialize a parser on the dataset. EM and HMM can get their input form there.
   * @param timeFrame The TimePeriod for which all articles will be loaded 
   * @param sparkContext the Spark Context
   * @param sourcePath the full path to the data (HDFS or local)
   * @throws NumberFormatException parser expected a number and found something else.
   * @throws XMLStreamException the xml file has unexpected format.
   * @throws ParseException could not parse a date.
   * @throws IOException other problems.
   */
  public InputParser(TimePeriod timeFrame,
          JavaSparkContext sparkContext,
          List<String> sourcePath) 
                  throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    this.timeFrame = timeFrame;
    
    List<String> sourceList = new LinkedList<String>();
    sourceList.addAll(sourcePath);
    
    JavaRDD<RawArticle> rawArticles = getRawArticleRdd(timeFrame, sourceList, sparkContext);
    segmentedArticles = rawArticles.map(new SegmentArticle());
    
    backgroundModel = new BackgroundModel(segmentedArticles);
  }
  
  /** Returns the input for the EMAlgorithm.
    * @param partitioning a partitioning over the timeFrame. All timePeriod in the
    *        list should be included in the timeFrame used to construct the InputParser.
    * @return container for the background model and word
    *         count of every individual article in each stream.
   **/
  public EmInputFromParser getEmInput(List<TimePeriod> partitioning) {
    
    for (TimePeriod tp : partitioning) {
      if (!this.timeFrame.contains(tp)) {
        throw new IllegalArgumentException("Partition TimePeriod not contained in "
                                            + "the timeFrame of this Parser");
      }
    }
    
    return new EmInputFromParser(backgroundModel, segmentedArticles, partitioning);
  }
  
  /**Returns the input for the HMM Algorithm.
   * @return the input of the HMM Algo.
   */
  public HmmInputFromParser getHmmInput() {  
    return new HmmInputFromParser(backgroundModel, segmentedArticles, timeFrame);
  }
  
  // TODO: Do this out of master node
  private JavaRDD<RawArticle> getRawArticleRdd(
          final TimePeriod englobingTimePeriod,
          List<String> sourceList,
          JavaSparkContext sparkContext) 
          throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    List<String> sourcePaths = new LinkedList<String>();
    for (String folder: sourceList) {
      for (String fileName: englobingTimePeriod.getFilesNames()) {
        sourcePaths.add(folder + '/' + fileName);
      }
    }
    
    JavaRDD<String> pathsRdd = sparkContext.parallelize(sourcePaths, sourcePaths.size());
    
    
    JavaRDD<RawArticle> rawArticles = pathsRdd.flatMap(new FlatMapFunction<String, RawArticle>() {
      @Override
      public Iterable<RawArticle> call(String path) throws Exception {
        RawArticleInputStream ras = new RawArticleInputStream(englobingTimePeriod, path);
        RawArticle rawArticle;
        List<RawArticle> rawArticleList = new LinkedList<RawArticle>();
        while ((rawArticle = ras.read()) != null) {
          rawArticleList.add(rawArticle);
        }
        return rawArticleList;
      }  
    });
    
    return rawArticles; 
  }
}


/** Acts as an intermediary processing step between a RawArticle and a ParsedArticle.
 * The SegmentedArticle contains the ordered list of words constituting the original
 * article text. It also has a list of TimePeriods it belongs to. **/
@SuppressWarnings("serial")
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
  public static final String WORD_SPLIT_PATTERN = "[^\\p{L}]+";
  
  /** Splits the RawArticle's text into a list of words; turn result into a SegmentedArticle.
   * instance **/
  public SegmentedArticle call(RawArticle article) {
    
    String[] words = article.fullText.split(WORD_SPLIT_PATTERN);
    LinkedList<String> cleanedWords = new LinkedList<String>();
    for (String word : words) {
      if (word.length() > 0) {        
        cleanedWords.add(word.toLowerCase());
      }
    }
    return new SegmentedArticle(cleanedWords, article.stream, 
            article.issueDate);
  }
}