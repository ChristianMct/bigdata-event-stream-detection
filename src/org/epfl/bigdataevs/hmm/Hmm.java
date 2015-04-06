package org.epfl.bigdataevs.hmm;

import java.util.List;
import java.util.Map.Entry;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.commons.collections.list.TreeList;




/**A Hidden Markov Model (HMM) built to perform analysis of theme life cycles (paper section 4).
 * An HMM can be trained using train() or used to decode a text stream using decode() ;
 *  all the other steps of theme life cycles analysis should not be done in this class.
 *  
 * @author team Damien-Laurent-Sami
 *
 */
public class Hmm {
  
  
  
  private final int k;
  private final int n;
  private final int m;
  
  private TreeList outputAlphabet;
  
  private double[] pi;
  private double[][] a;
  private double[][] b;
  
  
  /**The basic hmm constructor.
   * 
   * 
   * @param k the number of transcollection-themes considered, the HMM will have k+1 states
   * @param wordsLexicon a rdd containing all the words in the vocabulary set (only once)
   * @param models the k+1 models : background model (index 0) 
   *      and the k trans-collection theme probabilities
   */
  public Hmm(int k, JavaRDD<String> wordsLexicon,
          JavaPairRDD<Integer,JavaPairRDD<String,Double>> models) {
    this.k = k;
    n = k + 1;
    outputAlphabet = new TreeList(wordsLexicon.collect());
    
    m = outputAlphabet.size();
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
  
  /**Dummy constructor for test purposes.
   * 
   */
  public Hmm() {
    k = 2;
    n = k + 1;
    m = 0; //to be modified
    //TODO outputAlphabet,b
    
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
  public JavaRDD<Integer> decode(JavaRDD<String> fullArticleStream) {
    //TODO implement decode
    
    return null;
    
  }
  

  public int getK() {
    return k;
  }

 
  public List<String> getOutputAlphabet() {
    return outputAlphabet;
  }


  

 
   
  

}
