package org.epfl.bigdataevs.hmm;

import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * A class that encapsulates all the tools necessary to perform theme life cycle analysis (paper
 * section 4).
 * 
 * @author team Damien-Laurent-Sami
 *
 */
public class LifeCycleAnalyser {

  private final int halfWindowInTimeIntervalUnit;

  private Hmm hmm;
  List<Integer> stateSequence;

  private HmmInput hmmInput;
  int numberOfTheme;

  /**
   * Basic LifeCycleAnalyser constructor.
   * 
   * @param halfWindowInTimeIntervalUnit
   *          half size of the window in time interval unit
   */
  public LifeCycleAnalyser(int halfWindowInTimeIntervalUnit) {
    this.halfWindowInTimeIntervalUnit = halfWindowInTimeIntervalUnit;
  }

  /**
   * Method that builds the HMM and does all the learning. This method needs to be called before any
   * visualization tool.
   * 
   * @param hmmInput
   * 
   */
  public void analyze(HmmInput hmmInput, JavaRDD<String> wordsLexicon,
          JavaPairRDD<Integer, JavaPairRDD<String, Double>> models) {
    // construct the HMM by calling constructor
    this.hmmInput = hmmInput;

    this.numberOfTheme = hmmInput.themesList.size();
    hmm = new Hmm(this.numberOfTheme, wordsLexicon, models);

    // train the HMM
    JavaRDD<String> fullArticleStream = null;
    hmm.train(fullArticleStream);

    // decode the sequence with the HMM
    stateSequence = hmm.decode(fullArticleStream).collect();

    // exploit the states sequence to measure the strength of each theme over time

  }

  /**
   * The absolute strength of a theme at each time period is measured by the number of words
   * generated by this theme in the documents corresponding to this time period, normalized by the
   * number of time points.
   * 
   * @param ithTheme
   *          index of the theme which absolute strength we want to calculate
   * @param timeIntervalNb
   *          index of the time interval at the center of the time window
   * @return absolute strength of the considered theme in this time window
   * 
   */
  public double astrength(int ithTheme, int timeIntervalNb) {
    // Determine first and last word the time window covers
    List<Integer> wordsInTimeIntervals = hmmInput.wordsInTimeIntervals;
    int indexOfFirstWordInWindow = 0;
    int indexOfLastWordInWindow = 0;
    int accumulator = 0;
    for (int i = 0; i <= timeIntervalNb + halfWindowInTimeIntervalUnit; i++) {
      if (i == timeIntervalNb - halfWindowInTimeIntervalUnit) {
        indexOfFirstWordInWindow = accumulator;
      }
      accumulator += wordsInTimeIntervals.get(i);
    }
    indexOfLastWordInWindow = accumulator;

    // Calculate the AStrength which is the number of word in the time period which were
    // classified as the theme we're interested in by the Viterbi algo (hmm.decode())
    int sumOfWordsClassifiedAsOurTheme = 0;
    List<Integer> ourStateSequence = stateSequence.subList(indexOfFirstWordInWindow,
            indexOfLastWordInWindow);
    Iterator<Integer> it = ourStateSequence.iterator();
    while (it.hasNext()) {
      if (it.next() == ithTheme) {
        sumOfWordsClassifiedAsOurTheme++;
      }
    }
    return (1.0 / (2.0 * halfWindowInTimeIntervalUnit)) * sumOfWordsClassifiedAsOurTheme;
  }

  /**
   * Summation of absolute strength of all themes in a given window.
   * 
   * @param timeIntervalNb
   *          middle point in the time window
   * @return the sum
   */
  public double computeAStrengthSum(int timeIntervalNb) {
    double sum = 0;
    for (int i = 0; i < numberOfTheme; i++) {
      sum += astrength(i, timeIntervalNb);
    }
    return sum;
  }

  /**
   * Relative strength of a theme.
   * 
   * @param ithTheme
   *          index of the theme which relative strength we want to calculate
   * @param timeIntervalNb
   *          middle point in the time window
   * @param sum
   *          Sum of the absolute strength of all themes in the time window
   * @return relative strength of the considered theme in this time window
   */
  public double nstrength(int ithTheme, int timeIntervalNb, double sum) {
    return astrength(ithTheme, timeIntervalNb) / sum;
  }

  // TODO visualization tools

}
