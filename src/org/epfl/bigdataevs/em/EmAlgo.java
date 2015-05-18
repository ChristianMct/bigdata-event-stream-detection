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
import org.epfl.bigdataevs.executables.Parameters;

import scala.Array;
import scala.Tuple2;

import java.io.Serializable;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;



/**
 * Team Antoine and Nina
 * 
 * @author abastien
 *
 * Perform several runs of the EM algorithm.
 * Select the best run according to the log-likelihoods.
 * Output Theme with average score in the time period.
 * 
 * The data which is processed by the EM Algorithm is a RDD of EmInput.
 */
public class EmAlgo implements Serializable {
  public final Map<String, BigFraction> backgroundModel;
  public JavaRDD<EmInput> partitions;
  public int numberOfThemes;
  public double lambdaBackgroundModel;
  public int numberOfRuns;
  public int numPartitions;

  
  
  /**
   * Creates one instance of EmAgorithm
   * Duplicates all partitions to the number of trials the algorithm should do.
   */
  public EmAlgo(JavaSparkContext sparkContext, EmInputFromParser collectionData){
    this(sparkContext, collectionData, Parameters.numberOfThemes,
            Parameters.lambdaBackgroundModel, Parameters.numberOfRunsEmAlgorithm);
  }
  /**
   * Creates one instance of EmAgorithm
   * Duplicates all partitions to the number of trials the algorithm should do.
   * @param partitions EmInputs
   * @param numThemes number of themes
   * @param lambda mixing weight of background model
   * @param numRuns number of trials for each EmInput
   */
  public EmAlgo(JavaSparkContext sparkContext, EmInputFromParser collectionData,
          int numThemes, double lambda,  int numRuns) {
    this.numberOfThemes = numThemes;
    this.lambdaBackgroundModel = lambda;
    this.numberOfRuns = numRuns;
    this.backgroundModel = collectionData.backgroundModel.backgroundModelRdd.collectAsMap();
    this.numPartitions = (int)collectionData.timePartitions.count();
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
    
   
    this.partitions = this.partitions.repartition(this.numPartitions);
      
  }
 
  /**
   * Creates one instance of EmAgorithm based on EmInputs
   * Duplicates all partitions to the number of trials the algorithm should do.
   * Use it only for testing
   * @param partitions EmInputs
   * @param numThemes number of themes
   * @param lambda mixing weight for background model
   * @param numRuns number of trials for one EmInput
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
    JavaRDD<EmInput> initializedPartitions = this.partitions.map(new Function<EmInput, EmInput>() {
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
    
    

    /*Loop of the algorithm*/
    return initializedPartitions.mapToPair(
            new PairFunction<EmInput, EmInput, Double>() {
      
          public boolean checkStoppingCondition(int iter) {
            return iter > Parameters.numberOfIterationsEmAlgorithm;          
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
              iteration += 1;
              input.numberOfIterations += 1;
            }
            
            input.sortArticlesByScore();           
            return new Tuple2<EmInput, Double>(input,
                    input.computeLogLikelihood(lambdaBackgroundModel));
          }
        });
  }
  
  /**
   * Performs a run of EM algorithm to every EmInputs
   * Select the best EmInput for EmInputs having the same indexOfPartition.
   * Provide filtered themes for all EmInputs
   * @return all themes with average score
   */
  public JavaPairRDD<Theme, Double> run() {
    
    JavaPairRDD<EmInput, Double> temp = this.algorithm();   
    temp.persist(StorageLevel.MEMORY_AND_DISK());
    
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
