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
  
  public EmAlgo(JavaRDD<EmInput> partitions) {
    this.partitions = partitions;
  }
  
  public void initialization() {
    // Initialize probabilities to all documents
    
  }
  
  public JavaPairRDD<Theme, Double> algo() {
    // Creation of RDD
    final int num = numberOfThemes;
    final double lambdaB = lambdaBackgroundModel;
    
    /**Initialize the themes*/
    ArrayList<Theme> themesList = new ArrayList<Theme>();
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
    
    
    /**Initialize probabilities that document d belongs to theme j*/
    this.partitions.foreach(new VoidFunction<EmInput>() {
      @Override
      public void call(EmInput inputPartition) throws Exception {
        inputPartition.initializeArticlesProbabilities();
      }
    });
    
    /**Loop of Algorithm*/    
    JavaPairRDD result = partitions.flatMapToPair(new PairFlatMapFunction<EmInput, Theme, Double>() {
      
      @Override
      public Iterable<Tuple2<Theme, Double>> call(EmInput input) throws Exception {
        ArrayList<ParsedArticle> documents = input.parsedArticles;
        
        boolean stoppingCondition = false;
        while(!stoppingCondition) {
          for (ParsedArticle parsedArticle : documents) {
            parsedArticle.updateHiddenVariablesThemes();
            parsedArticle.updateHiddenVariableBackgroundModel(input.backgroundModel, lambdaB);
            parsedArticle.updateProbabilitiesDocumentBelongsToThemes();
          }
          input.updateProbabilitiesOfWordsGivenTheme(input.themesOfPartition);
        }
        
        HashMap<Theme, Double> themesWithAverageProbability = new HashMap<>();
        for (Theme theme : input.themesOfPartition) {
          double sum = 0.0;
          for (ParsedArticle parsedArticle : documents) {
            sum += parsedArticle.probabilitiesDocumentBelongsToThemes.get(theme).doubleValue();
          }
          double average = sum/documents.size();
        }
        return (Iterable<Tuple2<Theme, Double>>) themesWithAverageProbability;
      }
    });
    return result;
    
  }
}
