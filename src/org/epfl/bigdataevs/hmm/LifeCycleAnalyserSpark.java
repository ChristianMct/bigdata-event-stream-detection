package org.epfl.bigdataevs.hmm;

import org.apache.commons.math3.fraction.BigFraction;
import org.epfl.bigdataevs.em.Theme;
import org.epfl.bigdataevs.eminput.BackgroundModel;
import org.epfl.bigdataevs.eminput.HmmInputFromParser;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

public class LifeCycleAnalyserSpark {
  private ArrayList<double[]> themes;
  private JavaPairRDD<Long, Long> wordStream;
  private JavaPairRDD<String, Long> lexicon;
  private JavaPairRDD<Long, String> invertedLexicon;
  private JavaRDD<Long> mostLikelySequenceThemeShifts;
  private long numberOfTheme;
  private long numberOfWord;
  private Hmm2 hmm;

  /**
   * Class used to analyze themes life cycle.
   * 
   * @param hmmInput
   *          The hmmInput from which is going to be used the background model, the lexicon and the
   *          wordStream. Themes must be added before any analytics can be done,
   * 
   */
  public LifeCycleAnalyserSpark(HmmInputFromParser hmmInput) {
    this.wordStream = hmmInput.wordStream;
    this.lexicon = hmmInput.lexicon;
    getInvertedLexicon();
    numberOfTheme = 0L;
    numberOfWord = lexicon.count();
    themes = new ArrayList<double[]>();
    setBackgroundModelAsTheme(hmmInput.backgroundModel);
  }

  /**
   * Produce, train and decode the Hmm. Must be done after adding theme
   * and before calculating any strengths.
   * 
   * @param sc the spark context
   * @param piThreshold Threshold on pi
   * @param aaThreshold Threshold on a
   * @param maxIterations Max number of iterations
   */
  public void analyse(JavaSparkContext sc, double piThreshold, double aaThreshold,
          int maxIterations) {
    int numberHiddenStates = (int) (numberOfTheme + 1);

    int numberObservableOutputSymbols = (int) numberOfWord;

    // setting up initial state probability distribution
    double[] pi = new double[numberHiddenStates];
    double initialStateDistribution = 1.0f / numberHiddenStates;
    for (int i = 0; i < numberHiddenStates; i++) {
      pi[i] = initialStateDistribution;
    }

    // setting up state transition probability distribution
    double[][] stateTransitionProbabilityDistribution =
            new double[numberHiddenStates][numberObservableOutputSymbols];
    double halfInitialStateDistribution = initialStateDistribution / 2.0;
    for (int i = 0; i < numberHiddenStates; i++) {
      for (int j = 0; j < numberObservableOutputSymbols; j++) {
        if (i == 0 && j == 0) {
          // .5 chance to stay in the background model
          stateTransitionProbabilityDistribution[i][j] = 0.5f;
        } else if (i == 0) {
          // equal chance to go from the background model to any other state
          stateTransitionProbabilityDistribution[i][j] = halfInitialStateDistribution;
        } else if (j == 0) {
          // .5 chance to return in the background model
          stateTransitionProbabilityDistribution[i][j] = 0.5f;
        } else if (i == j) {
          // .5 chance to return in the same state
          stateTransitionProbabilityDistribution[i][j] = 0.5f;
        } else {
          // no chance to go between any two different states
          stateTransitionProbabilityDistribution[i][j] = 0.0f;
        }
      }
    }

    // setting up output probability distribution
    double[][] outputProbabilityDistribution = (double[][]) themes.toArray();

    // setting up and training the hmm
    hmm = new Hmm2(numberHiddenStates, numberObservableOutputSymbols, pi,
            stateTransitionProbabilityDistribution, outputProbabilityDistribution);

    JavaRDD<Tuple2<Integer, Integer>> observedSequenceRdd = wordStream
            .map(new Function<Tuple2<Long, Long>, Tuple2<Integer, Integer>>() {

              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<Integer, Integer> call(Tuple2<Long, Long> wordEntry) throws Exception {
                return new Tuple2<Integer, Integer>(new Integer(wordEntry._1.intValue()),
                        new Integer(wordEntry._2.intValue()));
              }
            });

    hmm.rawSparkTrain(sc, observedSequenceRdd, piThreshold, aaThreshold, maxIterations, null);

    JavaRDD<Long> wordStreamWithoutTimeStamp = wordStream
            .map(new Function<Tuple2<Long, Long>, Long>() {

              private static final long serialVersionUID = 1L;

              @Override
              public Long call(Tuple2<Long, Long> wordEntry) throws Exception {
                return new Long(wordEntry._1.longValue());
              }
            });

    mostLikelySequenceThemeShifts = hmm.decode(wordStreamWithoutTimeStamp);
  }

  /**
   * Calculate absolute strength of a theme.
   * 
   * @param themeIndex
   *          index of the selected theme (return value from addTheme())
   * @param startTime
   *          starting timestamp of the time period considered
   * @param window
   *          length of the time period considered
   * @return the absolute strength of the theme
   */
  public int absoluteStrength(final int themeIndex, final long startTime, final long window) {
    JavaPairRDD<Tuple2<Long, Long>, Long> indexedWordStream = wordStream.zipWithIndex();
    JavaPairRDD<Tuple2<Long, Long>, Long> wordStreamInTheConsideredPeriod = indexedWordStream
            .filter(new Function<Tuple2<Tuple2<Long, Long>, Long>, Boolean>() {

              private static final long serialVersionUID = 1L;

              @Override
              public Boolean call(Tuple2<Tuple2<Long, Long>, Long> wordEntry) throws Exception {
                long endTime = startTime + window;
                long thisWordTimeStamp = wordEntry._1._2.longValue();
                return thisWordTimeStamp >= startTime && thisWordTimeStamp < endTime;
              }
            });

    final long minIndex = wordStreamInTheConsideredPeriod
            .min(new Comparator<Tuple2<Tuple2<Long, Long>, Long>>() {

              @Override
              public int compare(Tuple2<Tuple2<Long, Long>, Long> arg0,
                      Tuple2<Tuple2<Long, Long>, Long> arg1) {
                return (int) (arg0._2 - arg1._2);
              }

            })._2;
    final long maxIndex = wordStreamInTheConsideredPeriod
            .max(new Comparator<Tuple2<Tuple2<Long, Long>, Long>>() {

              @Override
              public int compare(Tuple2<Tuple2<Long, Long>, Long> arg0,
                      Tuple2<Tuple2<Long, Long>, Long> arg1) {
                return (int) (arg0._2 - arg1._2);
              }

            })._2;

    JavaPairRDD<Long, Long> indexedMostLikelySequenceThemeShifts = mostLikelySequenceThemeShifts
            .zipWithIndex();

    JavaRDD<Long> slicedMostLikelySequenceThemeShifts = indexedMostLikelySequenceThemeShifts
            .filter(new Function<Tuple2<Long, Long>, Boolean>() {
              private static final long serialVersionUID = 1L;

              @Override
              public Boolean call(Tuple2<Long, Long> wordEntry) throws Exception {
                return wordEntry._2 >= minIndex && wordEntry._2 <= maxIndex;
              }
            }).values();

    JavaRDD<Long> matchingTheme = slicedMostLikelySequenceThemeShifts
            .filter(new Function<Long, Boolean>() {
              private static final long serialVersionUID = 1L;

              @Override
              public Boolean call(Long thisThemeIndex) throws Exception {
                return thisThemeIndex.intValue() == themeIndex;
              }

            });

    return (int) matchingTheme.count();
  }

  private void getInvertedLexicon() {
    this.invertedLexicon = lexicon
            .mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {

              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<Long, String> call(Tuple2<String, Long> wordEntry) throws Exception {
                return new Tuple2<Long, String>(wordEntry._2, wordEntry._1);
              }
            });
  }

  private void setBackgroundModelAsTheme(BackgroundModel backgroundModel) {
    JavaPairRDD<String, BigFraction> backgroundModelRdd = backgroundModel.backgroundModelRdd;

    JavaRDD<Double> asDouble = backgroundModelRdd
            .map(new Function<Tuple2<String, BigFraction>, Double>() {

              private static final long serialVersionUID = 1L;

              @Override
              public Double call(Tuple2<String, BigFraction> wordEntry) throws Exception {
                return wordEntry._2.doubleValue();
              }
            });

    Double[] copyBeforeConversion = null;
    copyBeforeConversion = asDouble.collect().toArray(copyBeforeConversion);
    double[] backgroundModelOutputProbability = new double[(int) numberOfWord];
    for (int i = 0; i < numberOfWord; i++) {
      backgroundModelOutputProbability[i] = copyBeforeConversion[i];
    }

    themes.set(0, backgroundModelOutputProbability);
  }

  /**
   * Used to add all the themes before any analytics is done.
   * 
   * @param theme
   *          The theme to add
   * @return the theme index
   */
  public int addTheme(Theme theme) {
    double[] themeOutputProbability = new double[(int) numberOfWord];
    for (int i = 0; i < numberOfWord; i++) {
      String outputString = invertedLexicon.lookup((long) i).get(0);
      themeOutputProbability[i] = theme.wordsProbability.get(outputString);
    }
    themes.set((int) this.numberOfTheme + 1, themeOutputProbability);
    this.numberOfTheme++;

    return (int) this.numberOfTheme;
  }
}
