package org.epfl.bigdataevs.em;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.epfl.bigdataevs.eminput.ParsedArticle;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class EmAlgo {
  public JavaRDD<EmInput> partitions;
  public int numberOfThemes;
  public double lambdaBackgroundModel;
  
  public EmAlgo(JavaRDD<EmInput> partitions, int numThemes, double lambda) {
    this.partitions = partitions;
    this.numberOfThemes = numThemes;
    this.lambdaBackgroundModel = lambda;
  }
  
  
  public JavaPairRDD<Theme, Double> algo() {
    // Creation of RDD
    
    /**Initialize the themes*/
    this.partitions.zipWithIndex().foreach(new VoidFunction<Tuple2<EmInput,Long>>(){

      @Override
      public void call(Tuple2<EmInput, Long> arg) throws Exception {
        EmInput inputPartition = arg._1();
        Long index = arg._2();
        for (int i = 0; i < numberOfThemes; i++) {
          Theme theme = new Theme(new Date(), new Date());
          theme.initialization(inputPartition);
          inputPartition.addTheme(theme);
          theme.partitionIndex = index;
        }
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
    
    
    /**Initialize probabilities that document d belongs to theme j*/    
    this.partitions.foreach(new VoidFunction<EmInput>() {
      @Override
      public void call(EmInput inputPartition) throws Exception {
        inputPartition.initializeArticlesProbabilities();
      }
    });
    
    /**Loop of Algorithm*/    
    JavaPairRDD result = partitions.flatMapToPair(new PairFlatMapFunction<EmInput, Theme, Double>() {
      public int iterations = 0;
      public final static int MAX_ITERATIONS = 1000;
      public ArrayList<Double> logLikelihoods = new ArrayList<>();
      
      public boolean checkStoppingCondition() {
        return this.iterations >= MAX_ITERATIONS;
      }
      
      @Override
      public Iterable<Tuple2<Theme, Double>> call(EmInput input) throws Exception {
        ArrayList<ParsedArticle> documents = input.parsedArticles;
        
        while(!checkStoppingCondition()) {
          this.iterations += 1;
          for (ParsedArticle parsedArticle : documents) {
            parsedArticle.updateHiddenVariablesThemes();
            parsedArticle.updateHiddenVariableBackgroundModel(input.backgroundModel, lambdaBackgroundModel);
            parsedArticle.updateProbabilitiesDocumentBelongsToThemes();
          }
          input.updateProbabilitiesOfWordsGivenTheme(input.themesOfPartition);
          logLikelihoods.add(input.computeLogLikelihood(lambdaBackgroundModel));
        }
        
        HashMap<Theme, Double> themesWithAverageProbability = new HashMap<>();
        for (Theme theme : input.themesOfPartition) {
          double sum = 0.0;
          for (ParsedArticle parsedArticle : documents) {
            sum += parsedArticle.probabilitiesDocumentBelongsToThemes.get(theme).doubleValue();
          }
          double average = sum/documents.size();
          themesWithAverageProbability.put(theme, average);
        }
        return (Iterable<Tuple2<Theme, Double>>) themesWithAverageProbability;
      }
    });
    return result;
    
  }
}
