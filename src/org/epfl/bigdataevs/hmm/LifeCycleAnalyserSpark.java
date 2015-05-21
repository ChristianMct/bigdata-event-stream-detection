package org.epfl.bigdataevs.hmm;

import org.apache.commons.math3.fraction.BigFraction;
import org.epfl.bigdataevs.em.Theme;
import org.epfl.bigdataevs.input.BackgroundModel;
import org.epfl.bigdataevs.input.HmmInputFromParser;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

public class LifeCycleAnalyserSpark implements Serializable {
  // private ArrayList<double[]> themes;
  double[] bgAsArray;
  double[][] outputProbabilityDistribution = null;
  JavaPairRDD<Tuple2<Theme, Double>, Long> themesWithIndex;
  JavaRDD<Tuple2<Theme, ArrayList<Tuple2<Long, Long>>>> themesWithStrength;
  private JavaPairRDD<Long, Long> wordStream;
  private JavaPairRDD<String, Long> lexicon;
  private Map<String, Long> lexiconAsMap;
  private JavaPairRDD<Long, String> invertedLexicon;
  public JavaPairRDD<Integer, Integer> mostLikelySequenceThemeShifts;
  private long numberOfThemes;
  private long numberOfWords;
  public Hmm2 hmm;

  /**
   * Class used to analyze themes life cycle.
   * 
   * @param hmmInput
   *          The hmmInput from which is going to be used the background model, the lexicon and the
   *          wordStream. Themes must be added before any analytics can be done.
   * 
   */
  public LifeCycleAnalyserSpark(HmmInputFromParser hmmInput) {
    this.wordStream = hmmInput.wordStream;
    this.lexicon = hmmInput.lexicon;
    this.lexiconAsMap = lexicon.collectAsMap();
    getInvertedLexicon();
    numberOfThemes = 0L;
    numberOfWords = lexicon.count();
    // themes = new ArrayList<double[]>();
    setBackgroundModelAsThemebyId(hmmInput.backgroundModelById);
  }

  private void setBackgroundModelAsThemebyId(JavaPairRDD<Long, Double> backgroundModelById) {
    List<Tuple2<Long, Double>> bgCollected = backgroundModelById.collect();
    bgAsArray = new double[(int) numberOfWords];
    for (Tuple2<Long, Double> tuple : bgCollected) {
      bgAsArray[tuple._1.intValue()] = tuple._2;
    }
  }

  /**
   * Produce, train and decode the Hmm. Must be done after adding theme and before calculating any
   * strengths.
   * 
   * @param sc
   *          the spark context
   * @param piThreshold
   *          Threshold on pi
   * @param aaThreshold
   *          Threshold on a
   * @param maxIterations
   *          Max number of iterations
   */
  public void analyse(JavaSparkContext sc, double piThreshold,
          double aaThreshold, int maxIterations) {
    int numberHiddenStates = (int) (numberOfThemes + 1);

    int numberObservableOutputSymbols = (int) numberOfWords;

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

    // setting the output prob distribution
    outputProbabilityDistribution[0] = bgAsArray;

    // setting up and training the hmm
    if (outputProbabilityDistribution == null) {
      System.out.println("error : you need to specify the themes via"
              + "addAllThemesFromRDD before analyzing the sequence!");
    }
    hmm = new Hmm2(numberHiddenStates, numberObservableOutputSymbols, pi,
            stateTransitionProbabilityDistribution, outputProbabilityDistribution);

    if (outputProbabilityDistribution == null) {
      System.out
              .println("error : you need to specify the themes via addAllThemesFromRDD before analyzing the sequence!");
    }

    JavaPairRDD<Tuple2<Long, Long>, Long> wordStreamZippedWithIndex = wordStream.zipWithIndex();
    
    JavaRDD<Tuple2<Integer, Integer>> observedSequenceRdd = wordStreamZippedWithIndex.map(
            new Function<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Integer, Integer>>() {

              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<Integer, Integer> call(Tuple2<Tuple2<Long, Long>, Long> wordEntry)
                      throws Exception {
                return new Tuple2<Integer, Integer>(wordEntry._2.intValue(), wordEntry._1._1
                        .intValue());
              }
            });

    System.out.println("observedSequenceRdd length : " + observedSequenceRdd.count());
    System.out.println("observedSequenceRdd : "
            + Arrays.toString(Arrays.copyOf(observedSequenceRdd.collect().toArray(), 50)));
    hmm.rawTrain(sc, observedSequenceRdd, piThreshold, aaThreshold, maxIterations);

    mostLikelySequenceThemeShifts = hmm.decode(sc, observedSequenceRdd);
    
    JavaPairRDD<Integer, Tuple2<Long, Long>> wordStreamZippedWithIndexReversed = wordStreamZippedWithIndex
            .mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long>, Integer, Tuple2<Long, Long>>() {

              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<Integer, Tuple2<Long, Long>> call(Tuple2<Tuple2<Long, Long>, Long> arg0)
                      throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2<Integer, Tuple2<Long, Long>>(arg0._2.intValue(), arg0._1);
              }

            });
    
    JavaPairRDD<Integer, Tuple2<Integer, Tuple2<Long, Long>>> zippedDecodedSequence = 
            mostLikelySequenceThemeShifts.join(wordStreamZippedWithIndexReversed);
    
    JavaRDD<Tuple2<Long, Integer>> nonZeroMostLikelyByTimestamp = zippedDecodedSequence
            .flatMap(new FlatMapFunction<Tuple2<Integer, Tuple2<Integer, Tuple2<Long, Long>>>,
                    Tuple2<Long, Integer>>() {

              @Override
              public Iterable<Tuple2<Long, Integer>> call(
                      Tuple2<Integer, Tuple2<Integer, Tuple2<Long, Long>>> arg0) throws Exception {
                ArrayList<Tuple2<Long, Integer>> list = new ArrayList<Tuple2<Long, Integer>>(1);
                if (arg0._2._1 != 0) {
                  list.add(new Tuple2<Long, Integer>(arg0._2._2._2, arg0._2._1));
                }
                return list;
              }

            });

    JavaPairRDD<Long, Iterable<Tuple2<Long, Integer>>> groupedRdd =
            nonZeroMostLikelyByTimestamp.groupBy(new Function<Tuple2<Long, Integer>, Long>(){

      @Override
      public Long call(Tuple2<Long, Integer> arg0) throws Exception {
        return arg0._1;
      }
      
    });
    List<Long> timestampsList = groupedRdd.keys().collect();
    
    Collections.sort(timestampsList);
    
    
    
    JavaPairRDD<Long, Map<Integer, Integer>> resultByTimestamp = groupedRdd
            .mapValues(new Function<Iterable<Tuple2<Long, Integer>>, Map<Integer, Integer>>() {

              @Override
              public Map<Integer, Integer> call(Iterable<Tuple2<Long, Integer>> arg0)
                      throws Exception {
                Map<Integer, Integer> countMap = new HashMap<Integer, Integer>();
                for (Tuple2<Long, Integer> tuple : arg0) {
                  if (countMap.containsKey(tuple._2)) {
                    countMap.put(tuple._2, countMap.get(tuple._2) + 1);
                  } else {
                    countMap.put(tuple._2, 1);
                  }
                }
                return countMap;
              }

            });
    
    
    List<Tuple2<Long, Map<Integer, Integer>>> collectedResults = resultByTimestamp.collect();
    
    Collections.sort(collectedResults, new Comparator<Tuple2<Long, Map<Integer, Integer>>>() {
      @Override
      public int compare(Tuple2<Long, Map<Integer, Integer>> index1,
              Tuple2<Long, Map<Integer, Integer>> index2) {
        return index1._1.compareTo(index2._1);
      }
    });
    
    
    //Printing in the appropriate format for the csv file
    for (int themeIndex = 1; themeIndex < numberOfThemes; themeIndex++) {
      System.out.println("Theme " + (themeIndex-1));
      Iterator<Tuple2<Long, Map<Integer, Integer>>> resultsIterator = collectedResults.iterator();
      Iterator<Long> timestampsIterator = timestampsList.iterator();

      Tuple2<Long, Map<Integer, Integer>> currentTuple = resultsIterator.next();

      while (timestampsIterator.hasNext()) {
        long timestamp = timestampsIterator.next();
        if (currentTuple != null && timestamp == currentTuple._1) {
          int strength = currentTuple._2().get(themeIndex) == null ? 0:currentTuple._2().get(themeIndex) ;
          System.out.println(timestamp + "," + strength);
          if (resultsIterator.hasNext()) {
            currentTuple = resultsIterator.next();
          } else {
            currentTuple = null;
          }
          
        } else {
          System.out.println(timestamp + ",0");
        }

      }
    }
    
    
    //old way to do it
    int timeDuration = collectedResults.size();
    int[][] themesStrengths = new int[(int) numberOfThemes][timeDuration];
    
    for (int timeIndex = 0 ; timeIndex < timeDuration ; timeIndex++) {
      Tuple2<Long, Map<Integer, Integer>> tuple = collectedResults.get(timeIndex);
      for (Entry<Integer,Integer> entry : tuple._2.entrySet()) {
        themesStrengths[entry.getKey() - 1][timeIndex] = entry.getValue();        
      }
    }
    
    System.out.println("");
    for ( int i = 0; i < numberOfThemes; i++ ) {
      System.out.print("themeStrength_" + i + " = [");
      for ( int j = 0; j < timeDuration; j++ ) {
        System.out.print(" " + themesStrengths[i][j]);
      }
      System.out.println("];");
    }
    
  }

  public void allAbsoluteStrength(String path, final long startTime, final long endTime,
          final long window, final long numberOfValuePerTheme) {
    themesWithStrength = themesWithIndex
            .map(new Function<Tuple2<Tuple2<Theme, Double>, Long>, Tuple2<Theme, ArrayList<Tuple2<Long, Long>>>>() {

              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<Theme, ArrayList<Tuple2<Long, Long>>> call(
                      Tuple2<Tuple2<Theme, Double>, Long> themeEntry) throws Exception {
                ArrayList<Tuple2<Long, Long>> themeStrength = new ArrayList<Tuple2<Long, Long>>();
                Long themeIndex = themeEntry._2;
                for (int i = 0; i < numberOfValuePerTheme; i++) {
                  float pc = (float) i / numberOfValuePerTheme;
                  long localStartTime = (long) (startTime + pc * (endTime - startTime));
                  int strength = absoluteStrength(themeIndex.intValue(), localStartTime, window);
                  themeStrength.add(new Tuple2<Long, Long>((long) strength, localStartTime));
                }
                return new Tuple2<Theme, ArrayList<Tuple2<Long, Long>>>(themeEntry._1._1,
                        themeStrength);
              }
            });
    themesWithStrength.saveAsTextFile(path);
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

    JavaRDD<Long> wordStreamIndexOnly = wordStreamInTheConsideredPeriod
            .map(new Function<Tuple2<Tuple2<Long, Long>, Long>, Long>() {
              private static final long serialVersionUID = 1L;

              @Override
              public Long call(Tuple2<Tuple2<Long, Long>, Long> arg0) throws Exception {
                // TODO Auto-generated method stub
                return arg0._2;
              }

            });

    final long minIndex = wordStreamIndexOnly.fold(Long.MAX_VALUE,
            new Function2<Long, Long, Long>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Long call(Long arg0, Long arg1) throws Exception {
              if (arg0 < arg1) {
                return arg0;
              } else {
                return arg1;
              }
            }

          });
    final long maxIndex = wordStreamIndexOnly.fold(Long.MAX_VALUE,
            new Function2<Long, Long, Long>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Long call(Long arg0, Long arg1) throws Exception {
              if (arg0 > arg1) {
                return arg0;
              } else {
                return arg1;
              }
            }

          });

    JavaPairRDD<Integer, Integer> indexedMostLikelySequenceThemeShifts
          = mostLikelySequenceThemeShifts;

    JavaRDD<Integer> slicedMostLikelySequenceThemeShifts = indexedMostLikelySequenceThemeShifts
            .filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
              private static final long serialVersionUID = 1L;

              @Override
              public Boolean call(Tuple2<Integer, Integer> wordEntry) throws Exception {
                return wordEntry._2 >= minIndex && wordEntry._2 <= maxIndex;
              }
            }).values();

    JavaRDD<Integer> matchingTheme = slicedMostLikelySequenceThemeShifts
            .filter(new Function<Integer, Boolean>() {
              private static final long serialVersionUID = 1L;

              @Override
              public Boolean call(Integer thisThemeIndex) throws Exception {
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

  /*
   * private void setBackgroundModelAsTheme(BackgroundModel backgroundModel) { JavaPairRDD<String,
   * BigFraction> backgroundModelRdd = backgroundModel.backgroundModelRdd;
   * 
   * JavaRDD<Double> asDouble = backgroundModelRdd .map(new Function<Tuple2<String, BigFraction>,
   * Double>() {
   * 
   * private static final long serialVersionUID = 1L;
   * 
   * @Override public Double call(Tuple2<String, BigFraction> wordEntry) throws Exception { return
   * wordEntry._2.doubleValue(); } });
   * 
   * Double[] copyBeforeConversion = null; copyBeforeConversion =
   * asDouble.collect().toArray(copyBeforeConversion); double[] backgroundModelOutputProbability =
   * new double[(int) numberOfWords]; for (int i = 0; i < numberOfWords; i++) {
   * backgroundModelOutputProbability[i] = copyBeforeConversion[i]; }
   * 
   * themes.set(0, backgroundModelOutputProbability); }
   */
  /**
   * Used to add all the themes before any analytics is done.
   * 
   * @param theme
   *          The theme to add
   * @return the theme index
   */
  /*
   * public int addTheme(Theme theme) { double[] themeOutputProbability = new double[(int)
   * numberOfWords]; for (int i = 0; i < numberOfWords; i++) { String outputString =
   * invertedLexicon.lookup((long) i).get(0); themeOutputProbability[i] =
   * theme.wordsProbability.get(outputString); } themes.set((int) this.numberOfThemes + 1,
   * themeOutputProbability); this.numberOfThemes++; System.out.println("Je suis un fruit ;) ");
   * return (int) this.numberOfThemes; }
   */

  /**
   * Used to add all the themes at once from EM output, to be called before any analytics is done.
   * 
   * @param themesRdd
   */
  public void addAllThemesFromRdd(JavaPairRDD<Theme, Double> themesRdd) {
    themesWithIndex = themesRdd.zipWithIndex();

    // here is the print of the themes
    Map<Tuple2<Theme, Double>, Long> emOutputs = themesWithIndex.collectAsMap();

    List<Tuple2<Tuple2<Theme, Double>, Long>> orderedThemesWithIndex = themesWithIndex.collect();

    Collections.sort(orderedThemesWithIndex, new Comparator<Tuple2<Tuple2<Theme, Double>, Long>>() {
      @Override
      public int compare(Tuple2<Tuple2<Theme, Double>, Long> index1,
              Tuple2<Tuple2<Theme, Double>, Long> index2) {
        return index1._2.compareTo(index2._2);
      }
    });

    
    //NOTE : uncomment this if you want to detect and analyse only a specific theme
    /*
    final List<Integer> olympicsList = new ArrayList<Integer>();
    List<String> olympicsWords = new ArrayList<String>();
    olympicsWords.add("votations");
    olympicsWords.add("vote");
    olympicsWords.add("scrutins");
    olympicsWords.add("fédérales");
    olympicsWords.add("élections");
    olympicsWords.add("élection");
    olympicsWords.add("résultats");
    olympicsWords.add("cantons");
    olympicsWords.add("points");
    olympicsWords.add("peuple");
    olympicsWords.add("résultats");
    */
    
    
    System.out.println("Printing themes as ordered in the hmm");
    for (Tuple2<Tuple2<Theme, Double>, Long> tuple : orderedThemesWithIndex) {
      Tuple2<Theme, Double> theme = tuple._1;
      System.out.println("Theme :" + tuple._2);
      System.out.println(theme._1.sortTitleString(3));
      System.out.println(theme._1.sortString(12));
      System.out.println("Score: " + theme._2);
      System.out.println("");
      /* SEE NOTE ABOVE
      TreeMap<String, Double> importantWordsMap = theme._1.sortString(12);
      Set<String> importantWordsSet = importantWordsMap.keySet();
      importantWordsSet.retainAll(olympicsWords);
      if (importantWordsSet.size() >= 2) {
        olympicsList.add(tuple._2.intValue());
        System.out.println("**********************************************************************");
        System.out.println("");
        System.out.println("THEMES DETECTED");
        System.out.println("");
        System.out.println("***********************************************************************");
      }
      */
    }
    
    /*SEE NOTE ABOVE
    final List<Integer> olympicsListFinal = new ArrayList<Integer>(olympicsList);
     */
    JavaPairRDD<Long, double[]> themesToArray = themesWithIndex
            .mapToPair(new PairFunction<Tuple2<Tuple2<Theme, Double>, Long>, Long, double[]>() {

              @Override
              public Tuple2<Long, double[]> call(Tuple2<Tuple2<Theme, Double>, Long> arg0)
                      throws Exception {
                double[] b = new double[(int) numberOfWords];
                long themeId = arg0._2;
                Theme theme = arg0._1._1;
                Map<String, Double> wordsProbability = theme.wordsProbability;
                for (Entry<String, Double> entry : wordsProbability.entrySet()) {
                  int index = lexiconAsMap.get(entry.getKey()).intValue();
                  b[index] = entry.getValue();
                }

                return new Tuple2<Long, double[]>(themeId, b);
              }
            });
    
    
    /*SEE NOTE ABOVE
    JavaPairRDD<Long, double[]> themesToArray =  themesToArrayAux.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, double[]>, 
            Long, double[]>(){

              @Override
              public Iterable<Tuple2<Long, double[]>> call(Tuple2<Long, double[]> arg0)
                      throws Exception {
                List<Tuple2<Long, double[]>> list = new ArrayList<Tuple2<Long, double[]>>(1);
                if(olympicsListFinal.contains(arg0._1.intValue())){
                  list.add(arg0);
                }         
                return list;
              }
            });
            */
    
    
    List<Tuple2<Long, double[]>> themesList = themesToArray.collect();
    numberOfThemes = themesList.size();
    outputProbabilityDistribution = new double[(int) numberOfThemes + 1][(int) numberOfWords];

    System.out.println("numberOfThemes : "+numberOfThemes);
    int i =0;
    for (Tuple2<Long, double[]> tuple : themesList) {
      outputProbabilityDistribution[1 + i] = tuple._2;
      i++;
    }
  }

}
