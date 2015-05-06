package org.epfl.bigdataevs.em;

import org.apache.commons.math3.fraction.BigFraction;
import org.apache.commons.math3.fraction.Fraction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.epfl.bigdataevs.eminput.EmInputFromParser;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePartition;
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
import java.util.TreeMap;

public class EmAlgo implements Serializable {
  public final Map<String, BigFraction> backgroundModel;
  public JavaRDD<EmInput> partitions;
  public int numberOfThemes;
  public double lambdaBackgroundModel;
  public int numberOfRuns;
  public final static double epsilon = 0.0;
  public final static double precision = 1e-2;

  /**
   * Creates one instance of EmAgorithm
   * Duplicates all partitions to the number of trials the algorithm should do.
   * @param partitions
   * @param numThemes
   * @param lambda
   * @param numRuns
   */
  public EmAlgo(JavaSparkContext sparkContext, EmInputFromParser collectionData, int numThemes, double lambda,  int numRuns) {
    this.numberOfThemes = numThemes;
    this.lambdaBackgroundModel = lambda;
    this.numberOfRuns = numRuns;
    this.backgroundModel = collectionData.backgroundModel.backgroundModelRdd.collectAsMap();
    System.out.println("Background Model: " + this.backgroundModel.size());

    List<Integer> runs = new ArrayList<>();
    for (int i = 0; i < this.numberOfRuns; i++) {
      runs.add(i);
    }
    
    this.partitions = collectionData.timePartitions.zipWithIndex().map(
          new  Function<Tuple2<Tuple2<TimePeriod,TimePartition>,Long>, EmInput>() {
          @Override
          public EmInput call(Tuple2<Tuple2<TimePeriod, TimePartition>, Long> args1)
                  throws Exception {
            TimePartition inputTimePartition = args1._1._2;
            Long index = args1._2;
            EmInput emInput = new EmInput(inputTimePartition, backgroundModel);
            emInput.indexOfPartition = index;
            return emInput;
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
   * Creates one instance of EmAgorithm based on EmInputs
   * Duplicates all partitions to the number of trials the algorithm should do.
   * Use it only for testing
   * @param partitions
   * @param numThemes
   * @param lambda
   * @param numRuns
   */
  public EmAlgo(JavaSparkContext sparkContext, JavaRDD<EmInput> inputs, int numThemes, double lambda,  int numRuns) {
    this.numberOfThemes = numThemes;
    this.lambdaBackgroundModel = lambda;
    this.numberOfRuns = numRuns;
    this.backgroundModel = null;

    List<Integer> runs = new ArrayList<>();
    for (int i = 0; i < this.numberOfRuns; i++) {
      runs.add(i);
    }
    
    this.partitions = inputs.zipWithIndex().map(
          new  Function<Tuple2<EmInput, Long>, EmInput>() {
          @Override
          public EmInput call(Tuple2<EmInput, Long> args)
                  throws Exception {
            EmInput input = args._1;
            Long index = args._2;
            input.indexOfPartition = index;
            return input;
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
    System.out.println("EM RUN");
    /*Initialize the themes*/
    this.partitions = this.partitions.map(new Function<EmInput, EmInput>() {
      @Override
      public EmInput call(EmInput inputPartition) throws Exception {
        for (int i = 0; i < numberOfThemes; i++) {
          Theme theme = new Theme(inputPartition.timePeriod.from, inputPartition.timePeriod.to, i);
          theme.initialization(inputPartition);
          inputPartition.addTheme(theme);
          theme.partitionIndex = inputPartition.indexOfPartition;
        }
        inputPartition.initializeArticlesProbabilities();
        return inputPartition;
      }

    });
    
    

    /*Loop of the algorithm   
    JavaPairRDD<EmInput, Double> result = initializedPartitions.mapToPair(
            new PairFunction<EmInput, EmInput, Double>() {
      
          public boolean checkStoppingCondition(int iter) {
            return iter > 25;          
          }
      
          @Override
          public Tuple2<EmInput, Double> call(EmInput input) throws Exception {
            int iteration = 1;
            
            while (!checkStoppingCondition(iteration)) {
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
              //logLikelihoods.add(input.computeLogLikelihood(lambdaBackgroundModel)); 
              iteration += 1;
              input.numberOfIterations += 1;
            }
            
            input.sortArticlesByScore();           
            return new Tuple2<EmInput, Double>(input,
                    input.computeLogLikelihood(lambdaBackgroundModel));
          }
        });
    */
    JavaRDD<EmInput> temp = this.partitions;
    for (int i = 0; i < 25; i++) {
      JavaRDD<EmInput> temp2 = iteration(temp);
      //List<EmInput> between = temp2.collect();
      //System.out.println(between);
      System.out.println("Iteration " + i);  
      temp = temp2;
    }
    return temp.mapToPair(new PairFunction<EmInput, EmInput, Double>() {

      @Override
      public Tuple2<EmInput, Double> call(EmInput input) throws Exception {
        input.sortArticlesByScore();    
        return new Tuple2<EmInput, Double>(input, input.computeLogLikelihood(lambdaBackgroundModel));
      }
      
    });
  }
  
  
  public JavaRDD<EmInput> iteration(JavaRDD<EmInput> inputs) {
    return inputs.map(
            new Function<EmInput, EmInput>() {    
          @Override
          public EmInput call(EmInput input) throws Exception {
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
              return input;
            }           
          });
    
  }
  
  /**
   * Performs a run of EM algorithm to every EmInputs
   * Select the best EmInput for EmInputs having the same indexOfPartition.
   * @return all themes with average score
   */
  public JavaPairRDD<Theme, Double> run() {
    
    JavaPairRDD<EmInput, Double> temp = this.algorithm();   
    temp.cache();
    JavaPairRDD<TimePeriod, Tuple2<EmInput, Double>> processedPartitions = temp.mapToPair(
            new PairFunction<Tuple2<EmInput,Double>, TimePeriod, Tuple2<EmInput, Double>>() {
            public Tuple2<TimePeriod, Tuple2<EmInput, Double>> call(Tuple2<EmInput, Double> tuple)
                    throws Exception {
              return new Tuple2<TimePeriod, Tuple2<EmInput, Double>>(tuple._1.timePeriod, tuple);
            }
      });
    
    JavaRDD<EmInput> selectedPartitions = processedPartitions.groupByKey().map(
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
     
    return selectedPartitions.flatMapToPair(new PairFlatMapFunction<EmInput, Theme, Double>() {
      @Override
      public Iterable<Tuple2<Theme, Double>> call(EmInput input) throws Exception {
        return input.relatedFileteredThemes();
      }
    });
  }

}
