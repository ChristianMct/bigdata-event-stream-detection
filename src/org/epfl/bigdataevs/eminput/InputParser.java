package org.epfl.bigdataevs.eminput;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.epfl.bigdataevs.executables.Parameters;

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
  private final int pageNumberTreshold;
  private final JavaRDD<SegmentedArticle> segmentedArticles;
  private final TimePeriod timeFrame;
  private final BackgroundModel backgroundModel;
  
  /**Initialize a parser on the dataset. EM and HMM can get their input form there. Sets
   * the word discarding treshold to 5 occurence.Words having less than 5 occurences are
   * discarded from the background model, lexicon and HMM input. Sets the page number treshold to 4.
   * all articles starting at page greater than this treshold are discarded. The BackgroundModel is 
   * computed on the given timeFrame. Use the other constructor to set these values yourself.
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
    this(timeFrame, timeFrame,sparkContext,sourcePath,
            Parameters.numberOfCountsBackgroundModelThreshold,
            Parameters.firstNumberOfPagesInNewspaperThreshold);
  }
  
  /**Initialize a parser on the dataset. EM and HMM can get their input form there. You can
   * set the word discarding treshold. Words having less than this treshold occurences are
   * discarded from the background model, lexicon and HMM input. Use the other constructor
   * to set this value yourself.
   * @param timeFrame The TimePeriod of interest. (On which EM and HMM will be effectively runned).
   * @param backgroundModelTimeFrame The TimePeriod on which the backgroundModel should be
   *        constructed.Should include timeFrame.
   * @param sparkContext the Spark Context
   * @param sourcePath the full path to the data (HDFS or local)
   * @param wordDiscardTreshold minimum number of occurences for a word to appear in the
   *        backgroundModel, Lexicon, and HMM input
   * @param pageNumberTreshold the input parser will filter out articles that begin strictly after
   *        this treshold.
   * @throws NumberFormatException parser expected a number and found something else.
   * @throws XMLStreamException the xml file has unexpected format.
   * @throws ParseException could not parse a date.
   * @throws IOException other problems.
   */
  public InputParser(TimePeriod timeFrame,
          TimePeriod backgroundModelTimeFrame,
          JavaSparkContext sparkContext,
          List<String> sourcePath,
          int wordDiscardTreshold,
          int pageNumberTreshold) 
                  throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    if (!backgroundModelTimeFrame.contains(timeFrame)) {
      throw new IllegalArgumentException("Time frame for background model "
                                          + "does not contain time frame");
    }
    
    this.pageNumberTreshold = pageNumberTreshold;
    this.timeFrame = timeFrame;
    
    List<String> sourceList = new LinkedList<String>();
    sourceList.addAll(sourcePath);
    
    JavaRDD<RawArticle> rawArticles = getRawArticleRdd(backgroundModelTimeFrame, 
                                                       sourceList, 
                                                       sparkContext);
    JavaRDD<SegmentedArticle> segmentedArticlesForBackgroundModel = rawArticles
                                                                      .map(new SegmentArticle());
    
    backgroundModel = getBackgroundModel(wordDiscardTreshold);
    
    // Filter out the article that are not in the considered timeFrame and that have less than a
    // given number of words.
    segmentedArticles = segmentedArticlesForBackgroundModel
            .filter(new Function<SegmentedArticle, Boolean>() {
              @Override
              public Boolean call(SegmentedArticle article) throws Exception {
                return (InputParser.this.timeFrame.includeDates(article.publication)
                        && article.words.size() >= Parameters.numberOfWordsInArticlesThreshold);
              }
            });
  }
  
  /** Get a background model from the data given the word discarding treshold. EM and HMM
   * get their background model from their input getter: this method is meant to be for test
   * and data analysis
   * @param wordDiscardTreshold the minimum count for a word to be in this background model.
   * @return a new BackgroundModel
   */
  public BackgroundModel getBackgroundModel(int wordDiscardTreshold) {
    return new BackgroundModel(segmentedArticles, wordDiscardTreshold);
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
          if (rawArticle.pageNumber <= InputParser.this.pageNumberTreshold) {
            rawArticleList.add(rawArticle);
          }
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
  public final String title;
  
  protected SegmentedArticle( List<String> words, ArticleStream stream, 
          Date publication, String title) {
    this.words = words;
    this.stream = stream;
    this.publication = publication;
    this.title = title;
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
            article.issueDate, article.name);
  }
}