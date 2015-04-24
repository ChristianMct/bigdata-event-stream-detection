package org.epfl.bigdataevs.hmm;

import org.apache.spark.api.java.JavaPairRDD;
import org.epfl.bigdataevs.eminput.TimePeriod;

import java.sql.Time;
import java.util.List;
import java.util.Map;


/**This object collects all the input data necessary for the HMM phase.
 * The data from the parsing team is to be added via the constructor.
 * The output of the EM algo (i.e. the themes) is then added using the method setThemes.
 * 
 * @author team Damien-Laurent- Sami
 *
 */
public class HmmInput {  
  
  
  //data from parser
  TimePeriod period;
  JavaPairRDD<Integer,Integer> fullText;
  
  Time timeInterval; //this type can be changed if there exists something more suited
  List<Integer> wordsInTimeIntervals; //stores the number of words in fullText between fixed amounts of time
  
  Map<String, Double> backgroundModel;//would be best to have the lexicon index of the words instead of a string
  
  Map<String, Integer> lexicon;//mapping between all the words and unique indexes (used in fullText)
  
  
  //data from EM output
  List<Map<String, Double>> themesList;
  
  
  //localy needed data
  boolean areThemesSet;
  
  
  public HmmInput(JavaPairRDD<Integer,Integer> fullTex,List<Integer> wordsInTimeIntervals,
          Map<String, Double> backgroundModel,Map<String, Integer> lexicon, Time timeInterval,TimePeriod period){
    this.fullText = fullText;
    this.backgroundModel = backgroundModel;
    this.wordsInTimeIntervals = wordsInTimeIntervals;
    this.lexicon = lexicon;
    this.timeInterval = timeInterval;
    areThemesSet = false;
    this.period = period;
    
  }
  
  
  public void setThemes(List<Map<String, Double>> themesList){
    this.themesList = themesList;
    areThemesSet = true;
  }
    
  

}
