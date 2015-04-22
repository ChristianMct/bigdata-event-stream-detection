package org.epfl.bigdataevs.em;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.epfl.bigdataevs.eminput.TimePeriod;

import scala.Array;
import scala.Tuple2;

import java.io.Serializable;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EmAlgo implements Serializable {
  public JavaRDD<EmInput> partitions;
  public int numberOfThemes;
  public double lambdaBackgroundModel;
  public int numberOfRuns;
  public final static double epsilon = 0.0;

  /**
   * Creates one instance of EmAgorithm
   * Duplicates all partitions to the number of trials the algorithm should do.
   * @param partitions
   * @param numThemes
   * @param lambda
   * @param numRuns
   */
  public EmAlgo(JavaSparkContext sparkContext, JavaRDD<EmInput> inputs, int numThemes, double lambda,  int numRuns) {
    this.numberOfThemes = numThemes;
    this.lambdaBackgroundModel = lambda;
    this.numberOfRuns = numRuns;
    
    List<Integer> runs = new ArrayList<>();
    for (int i = 0; i < this.numberOfRuns; i++) {
      runs.add(i);
    }
    
    this.partitions = inputs.zipWithIndex().map(new Function<Tuple2<EmInput,Long>, EmInput>() {
      @Override
      public EmInput call(Tuple2<EmInput, Long> args1) throws Exception {
        EmInput inputPartition = args1._1();
        Long index = args1._2();
        inputPartition.indexOfPartition = index;
        return inputPartition;
      }
    }).cartesian(sparkContext.parallelize(runs)).map(
            new Function<Tuple2<EmInput,Integer>, EmInput>() {
        @Override
        public EmInput call(Tuple2<EmInput, Integer> args2) throws Exception {
          Long index = args2._1.indexOfPartition;
          EmInput input = args2._1.clone();
          input.indexOfPartition = index;
          input.run = args2._2;
          return input;
        }
      });
  }

  /**
   * Performs one run of the EM algorithm to all the partitions (EmInput)
   * @return the log-likelihood after convergence
   */
  public JavaPairRDD<EmInput, Double> algorithm() {
    // Creation of RDD

    /*Initialize the themes*/
    JavaRDD<EmInput> initializedPartitions = this.partitions.map(new Function<EmInput, EmInput>() {
      @Override
      public EmInput call(EmInput inputPartition) throws Exception {
        for (int i = 0; i < numberOfThemes; i++) {
          Theme theme = new Theme(inputPartition.timePeriod.from, inputPartition.timePeriod.to);
          theme.initialization(inputPartition);
          inputPartition.addTheme(theme);
          theme.partitionIndex = inputPartition.indexOfPartition;
        }
        inputPartition.initializeArticlesProbabilities();
        return inputPartition;
      }

    });

    /*Loop of the algorithm*/    
    JavaPairRDD<EmInput, Double> result = initializedPartitions.mapToPair(
            new PairFunction<EmInput, EmInput, Double>() {
      
          public boolean checkStoppingCondition(ArrayList<Double> logLikelihoods) {
            int iteration = logLikelihoods.size();
            if (iteration>1) {
              return (logLikelihoods.get(iteration - 1)
                      - logLikelihoods.get(iteration - 2)) < 1e-6;
            } else {
              return false;
            }
          }
      
          @Override
          public Tuple2<EmInput, Double> call(EmInput input) throws Exception {
            ArrayList<Double> logLikelihoods = new ArrayList<>();
            
            while (!checkStoppingCondition(logLikelihoods)) {
              for (Document article : input.documents) {
                article.updateHiddenVariablesThemes();
              }
              for (Document article : input.documents) {
                article.updateHiddenVariableBackgroundModel(
                        input.backgroundModel, lambdaBackgroundModel);
              }
              for (Document article : input.documents) {
                article.updateProbabilitiesDocumentBelongsToThemes();
              }
              input.updateProbabilitiesOfWordsGivenTheme(input.themesOfPartition);
              logLikelihoods.add(input.computeLogLikelihood(lambdaBackgroundModel));              
            }
            
            System.out.println("Number of iterations: " + logLikelihoods.size());
            System.out.println("Log-likelihood: " + logLikelihoods.get(logLikelihoods.size() - 1));
            
            return new Tuple2<EmInput, Double>(input,
                    logLikelihoods.get(logLikelihoods.size() - 1));
          }
        });

    return result;
  }
  
  /**
   * Performs a run of EM algorithm to every EmInputs
   * Select the best EmInput for EmInputs having the same indexOfPartition.
   * @return all themes with average score
   */
  public JavaRDD<EmInput> run() {
    /*
    return this.algorithm().keys();
    */
    JavaPairRDD<TimePeriod, Tuple2<EmInput, Double>> processedPartitions = this.algorithm().mapToPair(
            new PairFunction<Tuple2<EmInput,Double>, TimePeriod, Tuple2<EmInput, Double>>() {
            public Tuple2<TimePeriod, Tuple2<EmInput, Double>> call(Tuple2<EmInput, Double> tuple)
                    throws Exception {
              return new Tuple2<TimePeriod, Tuple2<EmInput, Double>>(tuple._1.timePeriod, tuple);
            }
      });
    
    JavaPairRDD<TimePeriod,Iterable<Tuple2<EmInput,Double>>> rdd = processedPartitions.groupByKey();
    
    return processedPartitions.groupByKey().map(
            new Function<Tuple2<TimePeriod,Iterable<Tuple2<EmInput,Double>>>, EmInput>() {
            @Override
            public EmInput call(
                    Tuple2<TimePeriod, Iterable<Tuple2<EmInput, Double>>> iterable) throws Exception {
              Iterator<Tuple2<EmInput, Double>> it = iterable._2.iterator();
              Tuple2<EmInput, Double> bestInput = (Tuple2<EmInput, Double>) it.next();
              while (it.hasNext()) {
                Tuple2<EmInput, Double> currentInput = (Tuple2<EmInput, Double>) it.next();
                if (currentInput._2 > bestInput._2) {
                  bestInput = currentInput;
                }
              }
              return bestInput._1;
            }
          });
     
    
  }
  
  public JavaPairRDD<Theme, Double> relatedThemes(JavaRDD<EmInput> selectedInputs) {
    
    JavaPairRDD<Theme, Double> listOfSelectedThemes = selectedInputs.flatMapToPair(
            new PairFlatMapFunction<EmInput, Theme, Double>() {
          @Override
          public Iterable<Tuple2<Theme, Double>> call(EmInput input) throws Exception {
            List<Tuple2<Theme, Double>> themesWithAverageProbability = new ArrayList<>();                  
            for (Theme theme : input.themesOfPartition) {
              double sum = 0.0;
              for (Document article : input.documents) {
                sum += article.probabilitiesDocumentBelongsToThemes.get(theme);
              }
              double average = sum / input.documents.size();
              themesWithAverageProbability.add(new Tuple2<Theme, Double>(theme, average));
            }
            return (Iterable<Tuple2<Theme, Double>>) themesWithAverageProbability;
          }
        });
    return listOfSelectedThemes;
  }
  
  /**
   * Return a list of article that have the highest probability to belong to a theme.
   * @param inputs
   * @param nunmberArticlesPerTheme
   * @return
   */
  public JavaPairRDD<Theme, Iterable<Document>> relatedArticles(JavaRDD<EmInput> selectedInputs, 
          int nunmberArticlesPerTheme) {
    return selectedInputs.flatMapToPair(new PairFlatMapFunction<EmInput, Theme , Iterable<Document>>() {
      @Override
      public Iterable<Tuple2<Theme, Iterable<Document>>> call(EmInput input) throws Exception {
        
        List<Tuple2<Theme, Iterable<Document>>> themesWithArticles = new ArrayList<>();  
        return null;
      }
    });
  }

}
