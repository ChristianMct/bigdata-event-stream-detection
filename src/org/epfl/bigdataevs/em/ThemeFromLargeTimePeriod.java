package org.epfl.bigdataevs.em;

import org.apache.commons.math3.fraction.BigFraction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.epfl.bigdataevs.eminput.EmInputFromParser;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePartition;
import org.epfl.bigdataevs.eminput.TimePeriod;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Team : Antoine & Nina
 * 
 * @author Antoine
 * 
 * Cut TimePartition in several inputs and apply the EM algorithm on all EmInputs
 * The goal is to get all meaningful theme that are in this TimePartition
 * @author abastien
 *
 */
public class ThemeFromLargeTimePeriod implements Serializable {
  public EmAlgo emAlgorithm;
  public final int numberOfSplits;
  public final Map<String, BigFraction> backgroundModel;
  public Map<Theme, Double> themes;
  
  public ThemeFromLargeTimePeriod(JavaSparkContext sparkContext, 
          EmInputFromParser collectionData, 
          int numThemes, double lambda, int numRuns, final int numSplits) {
    
    this.numberOfSplits = numSplits;
    this.backgroundModel = collectionData.backgroundModel.backgroundModelRdd.collectAsMap();
    
    //Transform one EmInput in several inputs 
    JavaPairRDD<TimePeriod, TimePartition> timePartition = collectionData.timePartitions;
    if (timePartition.count() > 1) {
      throw new IllegalArgumentException(
              "ThemeFromLargeTimePeriod should only be given one (TimePeriod, TimePartition)");
    }
    
    
    JavaRDD<EmInput> inputs = timePartition.flatMap(
          new FlatMapFunction<Tuple2<TimePeriod,TimePartition>, EmInput>() {

            @Override
            public Iterable<EmInput> call(
                    Tuple2<TimePeriod, TimePartition> tuple) throws Exception {
              List<EmInput> output = new ArrayList<EmInput>();
              
              List<ParsedArticle> articles = new ArrayList<>(tuple._2.parsedArticles);
              int numberOfArticlesPerNewPartition = 
                      (int) Math.ceil(articles.size() / numberOfSplits);
              for (int i = 0; i < numberOfSplits; i++) {
                int start = numberOfArticlesPerNewPartition * i;
                int end = Math.min(
                        numberOfArticlesPerNewPartition * (i + 1) - 1, articles.size() - 1);
                List<ParsedArticle> subList = articles.subList(start, end);
                output.add(new EmInput(new TimePartition(subList, tuple._1), backgroundModel));
              }
              return output;
            }
      });
    
    EmAlgo emAlgorithm = new EmAlgo(sparkContext, inputs, numThemes, lambda, numRuns);
    JavaRDD<EmInput> result = emAlgorithm.algorithm().keys();
    
    this.themes = result.flatMapToPair(new PairFlatMapFunction<EmInput, Theme, Double>() {
      @Override
      public Iterable<Tuple2<Theme, Double>> call(EmInput input) throws Exception {
        return input.relatedThemes();
      }      
    }).collectAsMap();
    
    
  }
  
  /**
   * Selects numberOfThemes that have the highest score
   * @param numberOfThemes
   * @return
   */
  public List<Theme> selectThemes(int numberOfSelectedThemes) {
    TreeMap<Theme, Double> sortedMap = new TreeMap<>(new ValueComparator(this.themes));
    sortedMap.putAll(this.themes);
    
    List<Theme> output = new ArrayList<Theme>();
    int count = 0;
    for (Theme theme : sortedMap.keySet()) {
      count++;
      if (count > numberOfSelectedThemes) {
        break;
      } else {
        output.add(theme);
      }    
    }
    return output;
  }
  
  /**
   * Comparator to sort the articles according to their probability in decreasing order.
   *
   */
  class ValueComparator implements Comparator<Theme> {

    Map<Theme, Double> base;
    
    public ValueComparator(Map<Theme, Double> base) {
      this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(Theme a, Theme b) {
      if (base.get(a).compareTo(base.get(b)) == 1) {
        return -1;
      } else {
        return 1;
      } // returning 0 would merge keys
    }
  }
}
