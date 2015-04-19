package org.epfl.bigdataevs.em;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.epfl.bigdataevs.eminput.ParsedArticle;

import scala.Array;
import scala.Tuple2;

import java.io.Serializable;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class EmAlgo implements Serializable {
  public JavaRDD<EmInput> partitions;
  public int numberOfThemes;
  public double lambdaBackgroundModel;
  public final static MathContext mathContext = new MathContext(10, RoundingMode.HALF_EVEN); 
  public final static double epsilon = 0.0;//1e-9;

  public EmAlgo(JavaRDD<EmInput> partitions, int numThemes, double lambda) {
    this.partitions = partitions;
    this.numberOfThemes = numThemes;
    this.lambdaBackgroundModel = lambda;
  }


  public JavaPairRDD<Theme, Double> algo() {
    // Creation of RDD

    /**Initialize the themes*/
    JavaRDD<EmInput> initilizedPartitions = this.partitions.zipWithIndex().map(new Function<Tuple2<EmInput,Long>, EmInput>() {

      @Override
      public EmInput call(Tuple2<EmInput, Long> arg) throws Exception {
        EmInput inputPartition = arg._1();
        Long index = arg._2();
        for (int i = 0; i < numberOfThemes; i++) {
          Theme theme = new Theme(inputPartition.timePeriod.from, inputPartition.timePeriod.to);
          theme.initialization(inputPartition);
          inputPartition.addTheme(theme);
          theme.partitionIndex = index;
        }
        inputPartition.initializeArticlesProbabilities();
        System.out.println("Number of themes:" + inputPartition.themesOfPartition.size());
        return inputPartition;
      }

    });

    /*
    this.partitions.foreach(new VoidFunction<EmInput>() {

      @Override
      public void call(EmInput inputPartition) throws Exception {
        for (int i = 0; i < numberOfThemes; i++) {
          Theme theme = new Theme(new Date(), new Date());
          theme.initialization(inputPartition);
          inputPartition.addTheme(theme);
        }
      }
    });
     */

    System.out.println("Initialization done !");

    /**Loop of Algorithm*/    
    JavaPairRDD<Theme, Double> result = initilizedPartitions.flatMapToPair(
            new PairFlatMapFunction<EmInput, Theme, Double>() {

          public int iteration = 0;
          public final static int MAX_ITERATIONS = 100;
          public ArrayList<Double> logLikelihoods = new ArrayList<>();
      
          public boolean checkStoppingCondition() {
            if(iteration > 1) {
              return (this.logLikelihoods.get(iteration-1) - this.logLikelihoods.get(iteration-2)) < 1e-6;
            } else {
              return false;
            }
          }
      
          @Override
          public Iterable<Tuple2<Theme, Double>> call(EmInput input) throws Exception {
            ArrayList<ParsedArticle> documents = input.parsedArticles;
      
            while (!checkStoppingCondition()) {
              System.out.println("Iteration:"+iteration);
              for (ParsedArticle parsedArticle : documents) {
                parsedArticle.updateHiddenVariablesThemes();
              }
              System.out.println("Hidden variable updated");
              for (ParsedArticle parsedArticle : documents) {
                parsedArticle.updateHiddenVariableBackgroundModel(
                        input.backgroundModel, lambdaBackgroundModel);
              }
              System.out.println("Hidden background model updated");
              for (ParsedArticle parsedArticle : documents) {
                parsedArticle.updateProbabilitiesDocumentBelongsToThemes();
              }
              System.out.println("Prob in parsedArticles updated");
              input.updateProbabilitiesOfWordsGivenTheme(input.themesOfPartition);
              logLikelihoods.add(input.computeLogLikelihood(lambdaBackgroundModel));
              System.out.println("Prob in Themes updated");
              
              this.iteration += 1;
            }
            
            System.out.println("Number of iterations:" + this.logLikelihoods.size());
            for (double val : logLikelihoods) {
              System.out.println(val);
            }
            
            List<Tuple2<Theme, Double>> themesWithAverageProbability = new ArrayList<>();
            for (Theme theme : input.themesOfPartition) {
              double sum = 0.0;
              for (ParsedArticle parsedArticle : documents) {
                sum += parsedArticle.probabilitiesDocumentBelongsToThemes.get(theme).doubleValue();
              }
              double average = sum / documents.size();
              themesWithAverageProbability.add(new Tuple2<Theme, Double>(theme, average));
            }
            return (Iterable<Tuple2<Theme, Double>>) themesWithAverageProbability;
          }
        });

    System.out.println("Loop done !");
    return result;

  }
}
