package org.epfl.bigdataevs.executables;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.epfl.bigdataevs.em.EmAlgo;
import org.epfl.bigdataevs.em.EmInput;
import org.epfl.bigdataevs.em.Theme;
import org.epfl.bigdataevs.eminput.EmInputFromParser;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePartition;
import org.epfl.bigdataevs.eminput.TimePeriod;
import org.epfl.bigdataevs.evolutiongraph.EvolutionaryTransition;
import org.epfl.bigdataevs.evolutiongraph.KLDivergence;

import scala.Tuple2;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

public class EvolutionGraphTest {

  public static void main(String[] args) throws NumberFormatException, XMLStreamException, ParseException, IOException {
    System.out.println("STARTED TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    //sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    
    
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>(); 
    timePeriods.add(new TimePeriod(format.parse("1/2/1995-0"), format.parse("8/2/1995-0")));
    
    //System.out.println(timePeriods.get(0).includeDates(format.parse("1/1/1939-12")));
    
    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs:///projects/dh-shared/GDL/");
    //inputPaths.add("hdfs://user/christian/GDL");
 
    
    /*
    System.out.println("======Background model's content======");
    for(int background_word_id : result.backgroundWordMap.keySet()) {
      String background_word = result.backgroundWordMap.get(background_word_id);
      System.out.println(background_word
        + "(ID: " + background_word_id + "): " 
        + result.backgroundModel.get(background_word) + " distribution proba.");
    }
    
    
    System.out.println("======Word chronological list======");
    for (Integer word: result.collectionWords)
      System.out.println(word);
    */
    
    /*
     * Integration of the EM Algorithm
     */
    
    InputParser parser = new InputParser(TimePeriod.getEnglobingTimePeriod(timePeriods), ctx, inputPaths);
    EmInputFromParser emInputFromParser = parser.getEmInput(timePeriods);
    
    List<Integer> numArticles = emInputFromParser.timePartitions.map(new Function<Tuple2<TimePeriod,TimePartition>, Integer>() {

      @Override
      public Integer call(Tuple2<TimePeriod, TimePartition> v1) throws Exception {
        // TODO Auto-generated method stub
        return v1._2.parsedArticles.size();
      }
      
    }).collect();
    for (Integer integer : numArticles) {
      System.out.println("Number of articles : " + integer);
    }
    
    int numberOfThemes = 20;
    double lambdaBackgroundModel = 0.92;
    int numberOfRuns = 1;   
    EmAlgo emAlgo = new EmAlgo(ctx, emInputFromParser, numberOfThemes, lambdaBackgroundModel, numberOfRuns);
    
    JavaPairRDD<Theme, Double> themesRdd = emAlgo.run();    
    Map<Theme, Double> emOutputs = themesRdd.collectAsMap();
    
    System.out.println(emOutputs.keySet().size() + " elements");
    int i = 0;
    for (Theme theme : emOutputs.keySet()) {
      Tuple2<Integer, Integer> t = theme.statistics();
      System.out.println("Theme :" + i);
      System.out.println(theme.sortString(20));
      System.out.println("Score: " + emOutputs.get(theme));
      System.out.println("Stats 1:" + t._1 + " / " + t._2);
      System.out.println("Stats 2:" + theme.statistics2());
      i += 1;
    }
    

   /*
    
    KLDivergence kldivergence = new KLDivergence(1E10, 0.0001);
  
    System.out.println("KLDivergence starts");
    
    JavaRDD<EvolutionaryTransition> transitionGraph = 
        kldivergence.compute(themesRdd.map(
                new Function<Tuple2<Theme,Double>,Theme>(){
                  @Override
                  public Theme call(Tuple2<Theme, Double> arg0) throws Exception {
                    return(arg0._1());
                  }
                  
                }
        ));

    System.out.println("KLDivergence done");
    
    System.out.println("themesRdd = "+themesRdd.count());
    System.out.println("transitionGraph = "+transitionGraph.count());
    */
  }

}
