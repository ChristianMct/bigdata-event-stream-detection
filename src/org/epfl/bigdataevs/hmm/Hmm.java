package org.epfl.bigdataevs.hmm;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.collections.list.TreeList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;




/**A Hidden Markov Model (HMM) built to perform analysis of theme life cycles (paper section 4).
 * An HMM can be trained using train() or used to decode a text stream using decode() ;
 *  all the other steps of theme life cycles analysis should not be done in this class.
 * 
 * @author laurent
 *
 */
public class Hmm {
  
  private final Date beginningOfPeriod;
  private final Date endOfPeriod;
  
  private final int k;
  private final int n;
  private final int m;
  
  private TreeList outputAlphabet;
  
  private double[] pi;
  private double[][] a;
  private double[][] b;
  private ArrayList<HashMap<String,Double>> alternativeChoiceForB;
  
  
  /**The basic hmm constructor.
   * 
   * @param beginning the beginning date of the studied period
   * @param end the ending date of the studied period
   * @param k the number of transcollection-themes considered, the HMM will have k+1 states
   * @param wordsLexicon a rdd containing all the words in the vocabulary set (only once)
   */
  public Hmm(Date beginning, Date end, int k, JavaRDD<String> wordsLexicon,
          JavaPairRDD<Integer,JavaPairRDD<String,Double>> models) {
    beginningOfPeriod = beginning;
    endOfPeriod = end;
    this.k = k;
    n = k + 1;
    outputAlphabet = new TreeList(wordsLexicon.collect());
    
    m = (int) outputAlphabet.size();
    pi = new double[n];
    a = new double[n][n];
    b = new double[n][m];
    //TODO initialize b using the models provided as arguments
    for (Entry<Integer, JavaPairRDD<String, Double>> entry: models.collectAsMap().entrySet()) {
      int i = entry.getKey();
      for (Entry<String, Double> pair : entry.getValue().collectAsMap().entrySet()) {
        b[i][outputAlphabet.indexOf(pair.getKey())] = pair.getValue();
      }
    }
    
  }
  
  
  /**This method trains the HMM by performing the Baum-Welch algorithm.
   * 
   * @param fullArticleStream the full text of the concatenated articles
   */
  public void train(JavaRDD<String> fullArticleStream){
    //TODO implement train
    
    
  }
  
  
  /**This method associates a state of the HMM to each word of the stream using Viterbi algorithm.
   * 
   * @param fullArticleStream the full text of the concatenated articles
   * @return the sequence of HMM states associated with the stream : 
   *         each state is represented by an integer between 0 and k (0 for the background model)
   */
  public JavaRDD<Integer> decode(JavaRDD<String> fullArticleStream){
    //TODO implement decode
    
    return null;
    
  }
  
  
  
  
  
  
  

  public int getK() {
    return k;
  }

  public Date getBeginningOfPeriod() {
    return beginningOfPeriod;
  }

  public Date getEndOfPeriod() {
    return endOfPeriod;
  }



  public List<String> getOutputAlphabet() {
    return outputAlphabet;
  }


  

 
   
  

}
