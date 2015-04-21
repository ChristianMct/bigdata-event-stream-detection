package org.epfl.bigdataevs.eminput;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**@author Matias and Christian
 * Represents a processed data set of text articles. TextCollectionData contains the
 * background model retrieved from the original dataset and a RDD of EMInput instances 
 * (separated by time segments) which are used in the Expectation-Maximization (EM) and
 * the HMM phase.
 * 
 * TODO: Add RDD of EMInputs (must be computed from ParsedArticles set 
 *   in generateTextCollectionData)
 * **/
public class TextCollectionData {
  
  /** Map containing tuples of words (identified by their ID in backgroundWords) and their 
   * distribution in the streams. */
  public final Map<String, Fraction> backgroundModel;
  
  /** Identifies each distinct word instance with an int. **/
  public final BiMap<Integer, String> backgroundWordMap;
  
  /**Lists each word of every stream in chronological order.
   * Words are stored as integers for memory economy. To retrieve the text 
   * content of each word, use backgroundWordMap**/
  public final List<Integer> collectionWords;

  /** Time range considered for the data set. **/
  public final TimePeriod timeFrame;
  
  /** Article texts are split on anything not a letter or number **/
  public static final String WORD_SPLIT_PATTERN = "[^\\p{L}\\p{Nd}]+";
  
  /** Used for cleaning: words that have a count inferior or equal to this 
   * in the whole stream are filtered out**/
  public static final int WORD_COUNT_THRESHOLD = 3;


  /**Initializer from a background model and a collection of ParsedArticle.
   * @param IdWordMap a mapping that identifies an unique int to each distinct word
   *    in the dataset.
   * @param backgroundModel the background model of the whole dataset
   * @param parsedArticles TODO: make a JavaRDD of EMInputs out of that
   * @param forTimePeriod the TimeFrame that identifies the range of the dataset
   */
  public TextCollectionData(BiMap<Integer, String> IdWordMap,
          Map<String, Fraction> backgroundModel,
          JavaPairRDD<TimePeriod, ParsedArticle> parsedArticles,
          TimePeriod forTimePeriod,
          List<Integer> wordConcat) {
    this.backgroundWordMap = IdWordMap;
    this.timeFrame = forTimePeriod;
    this.backgroundModel = backgroundModel;
    //this.parsedArticles = parsedArticles;
    this.collectionWords = wordConcat;
  }
  
  
  /** Returns RDD mapping each (cleaned) word to its count in the whole dataset **/
  @SuppressWarnings("serial")
  private static JavaPairRDD<String, Integer> 
        createWordCountRDD(JavaRDD<SegmentedArticle> data) 
  {
    
    //temporary background model data: maps every word to its count for the whole time period
    JavaPairRDD<String, Integer> wordCountRdd = data
            .flatMapToPair(new PairFlatMapFunction<SegmentedArticle, String, Integer>() {
              
              public Iterable<Tuple2<String, Integer> > call(SegmentedArticle art) {
                LinkedList<Tuple2<String, Integer>> countTuples = 
                        new LinkedList<Tuple2<String, Integer>>();
                for (String word : art.words) {
                  countTuples.add(new Tuple2<String, Integer>(word, 1));
                }
                return countTuples;                
              }
              
            });
    
    //merge all equivalent words to the same key, adding their count
    JavaPairRDD<String, Integer> wordCountRddReduced = 
      wordCountRdd.reduceByKey(new Function2<Integer,
                  Integer,
                  Integer>() {  
            public Integer call(Integer lhs, Integer rhs) { 
              return lhs + rhs; 
            }   
            });
    
    //cleaning: filter out words that have exceptionally low occurence count
    return wordCountRddReduced.filter(
            new Function<Tuple2<String, Integer>, Boolean>() {          
              public Boolean call(Tuple2<String, Integer> tuple) {
                return tuple._2() > TextCollectionData.WORD_COUNT_THRESHOLD;
              }
            });
  }
  
  /** Returns the textual content of articleData as a list of words (represented by their
   * unique integer ID).
   * @param articleData JavaRDD of SegmentedArticle instances, must be chronologically
   * ordered
   * @param idCorrespondence the BiMap that translates a word into its id **/
  @SuppressWarnings("serial")
  private static List<Integer> generateWordConcatenation
    (JavaRDD<SegmentedArticle> articleData, final BiMap<String, Integer> idCorrespondence) 
  {
    //TODO: Check that collect() keeps ordering
    return articleData.flatMap(
            new FlatMapFunction<SegmentedArticle, Integer>() {
              
              public List<Integer> call(SegmentedArticle segArt) {
                List<Integer> textAsWordId = new LinkedList<Integer>();
                for (String word: segArt.words){
                  if (!idCorrespondence.containsKey(word))
                    textAsWordId.add(idCorrespondence.get(word));
                }
                return textAsWordId;
              }
            }).collect();   
  }
  
  
  
  /** Creates a TimePartition instance from an RDD containing all articles from all streams within
   * a particular time period.
   * @input the JavaRDD of RawArticle instances, contains every articles with their full text
   * @timeSegments the segmentation of time periods used for the EM phase. The periods
   *  can be overlapping but the list must be sorted in ascending order.
   * @return a TimePartition containing the background model for this time frame and the word
   *     count of every individual article. **/
  @SuppressWarnings("serial")
  public static TextCollectionData generateTextCollectionData(JavaRDD<RawArticle> input, 
          final List<TimePeriod> timeSegments) {
    
    /*Segments article's text into a list of words*/
    JavaRDD<SegmentedArticle> segmentedArticles = 
            input.map(new SegmentArticle(timeSegments));
    //TODO: how many partitions am I supposed to define??? See sortBy specification
    //it's probably expensive to do partitions().size() so we MUST change that
    segmentedArticles = segmentedArticles.sortBy(new Function<SegmentedArticle, Date>() { 
      public Date call(SegmentedArticle segArt) {
        return segArt.publication;
      }
       
    }, true, segmentedArticles.partitions().size());
    

    JavaPairRDD<String, Integer> cleanedWordCount = createWordCountRDD(segmentedArticles);
    
    //("usual" state) word -> word's count map
    Map<String, Integer> wordToCount = cleanedWordCount.collectAsMap();
    //word id -> word's count map
    Map<Integer, Integer> wordIdToCount = new HashMap<Integer, Integer>();
    //word-id -> string word map: Use BiMap because we want to also retrieve ids from words
    BiMap<Integer, String> IdWordMap = HashBiMap.create();
    int totalAmountCounter = 0;
    int wordId = 0;
    /*the following loop does two things: it counts the total amount of words
     * in the background model, and builds the id-to-word map (used to build 
     * backgroundWords attribute)
     * 
     * Probably a very expensive operation but it's certainly better to perform the two
     * calculations in one loop!
     */
    for (String word : wordToCount.keySet()) {
      int currentCount = wordToCount.get(word);
      totalAmountCounter += currentCount;
      IdWordMap.put(wordId, word);
      wordIdToCount.put(wordId, currentCount);
      wordId++;
    }
    
    final int totalAmount = totalAmountCounter;
    
    //turn wordCountRDDReduced into the final backgroundModelRDD (Map words to their distribution)
    //for each [word, word-count] pair, replace with [word, Fraction(word-count, total-word-count)]
    JavaPairRDD<String, Fraction>
        backgroundModelRdd = cleanedWordCount.mapValues(new Function<Integer, Fraction>() {
          public Fraction call(Integer count) {
            return new Fraction(count, totalAmount);
          }     
          });
    
    List<Integer> wordConcat = generateWordConcatenation(segmentedArticles, 
            IdWordMap.inverse());
    //TODO: remove cleaned words
    JavaPairRDD<TimePeriod, ParsedArticle> parsedArticles = 
            segmentedArticles.flatMapToPair(new ProcessArticle());

    return new TextCollectionData(IdWordMap,
            backgroundModelRdd.collectAsMap(),
            parsedArticles,
            TimePeriod.getEnglobingTimePeriod(timeSegments),
            wordConcat);
  }
  
}
  
/** Acts as an intermediary processing step between a RawArticle and a ParsedArticle.
 * The SegmentedArticle contains the ordered list of words constituting the original
 * article text. It also has a list of TimePeriods it belongs to. **/
class SegmentedArticle implements Serializable {
  
  public final List<String> words;
  public final ArticleStream stream;
  public final Date publication;
  public final List<TimePeriod> owningTimePeriods;
  
  public SegmentedArticle( List<String> words, ArticleStream stream, 
          Date publication, List<TimePeriod> owningTimePeriods) {
    this.words = words;
    this.stream = stream;
    this.publication = publication;
    this.owningTimePeriods = owningTimePeriods;
  }
  
}

@SuppressWarnings("serial")
class SegmentArticle implements Function<RawArticle, SegmentedArticle> {
  
  /** List of all the time partitions we will consider. Used to detect overlaps **/
  private final List<TimePeriod> timePeriods;
  
  public SegmentArticle(List<TimePeriod> timePeriods) {
    this.timePeriods = timePeriods;
  }

  /** Splits the RawArticle's text into a list of words; turn result into a SegmentedArticle
   * instance **/
  public SegmentedArticle call(RawArticle article) {         
    
    LinkedList<TimePeriod> containingPeriods = new LinkedList<TimePeriod>();
    for (TimePeriod segment : timePeriods) {
      if (segment.includeDates(article.issueDate))
        containingPeriods.add(segment);
    }
    //Article outside of parsed range: don't consider
    if (containingPeriods.isEmpty())
      return null;
    
    String[] words = article.fullText.split(TextCollectionData.WORD_SPLIT_PATTERN);
    LinkedList<String> cleanedWords = new LinkedList<String>();
    for (String word : words) {
      cleanedWords.add(word.toLowerCase());
    }
    return new SegmentedArticle(cleanedWords, article.stream, 
            article.issueDate, containingPeriods);
    
    //return new Tuple2<Date, SegmentedArticle>(article.issueDate, result);
  }
}


@SuppressWarnings("serial")
class ProcessArticle implements 
  PairFlatMapFunction<SegmentedArticle, TimePeriod, ParsedArticle> 
{

  /** Compute the count of each word in article. Then produce a ParsedArticle
   * and return a mapping of this ParsedArticle with each of its parent
   * TimePeriod partitions. **/
  public List<Tuple2<TimePeriod, ParsedArticle>> call(SegmentedArticle article) {         
    
    List<Tuple2<TimePeriod, ParsedArticle>> result = 
            new ArrayList<Tuple2<TimePeriod, ParsedArticle>>();
    HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
    
    for (String word : article.words) {
      Integer currentCount = wordCount.get(word);
      if (currentCount == null || currentCount <= 0) {
        currentCount = 1;
      } else {
        currentCount++;
      }

      wordCount.put(word, currentCount);
    }
    
    ParsedArticle parsedArticle = new ParsedArticle(wordCount, article.stream, 
            article.publication);
    for (TimePeriod articlePeriod: article.owningTimePeriods)
      result.add(new Tuple2<TimePeriod, ParsedArticle>(articlePeriod, parsedArticle));
      
    return result;
  }
}
