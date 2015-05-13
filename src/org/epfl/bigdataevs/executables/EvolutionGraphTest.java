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
import org.epfl.bigdataevs.em.LightTheme;
import org.epfl.bigdataevs.em.Theme;
import org.epfl.bigdataevs.em.ThemeFromLargeTimePeriod;
import org.epfl.bigdataevs.eminput.EmInputFromParser;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePartition;
import org.epfl.bigdataevs.eminput.TimePeriod;
import org.epfl.bigdataevs.evolutiongraph.EvolutionaryTransition;
import org.epfl.bigdataevs.evolutiongraph.GraphVisualization;
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
    

    Calendar c = Calendar.getInstance();
    Date startDate = format.parse("20/10/1992-0");
    c.setTime(startDate);
    for (int i = 0; i < 8; i++) {
      Date c1 = c.getTime();
      c.add(Calendar.DATE, 5);
      Date c2 = c.getTime();
      timePeriods.add(new TimePeriod(c1, c2));
      System.out.println(c1 + "-" + c2);
    }
    Date endDate = c.getTime();
    
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
    int wordsThreshold = Parameters.numberOfCountsBackgroundModelThreshold;
    int pageThreshold = Parameters.firstNumberOfPagesInNewspaperThreshold;
    
    InputParser parser = new InputParser(TimePeriod.getEnglobingTimePeriod(timePeriods), 
            ctx, inputPaths, wordsThreshold, pageThreshold);
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
    
    int numberOfThemes = Parameters.numberOfThemes;
    double lambdaBackgroundModel = Parameters.lambdaBackgroundModel;
    int numberOfRuns = Parameters.numberOfRunsEmAlgorithm;   
    EmAlgo emAlgo = new EmAlgo(ctx, emInputFromParser, numberOfThemes, lambdaBackgroundModel, numberOfRuns);
    
    JavaPairRDD<Theme, Double> themesRdd = emAlgo.run();   
    themesRdd.cache();
    
    /* The following code raises
     * 
     * java.lang.OutOfMemoryError: GC overhead limit exceeded
     * or
     * java.lang.OutOfMemoryError: Java heap space
     * 
     * set option '--driver-memory 32G' to avoid these 
     */
    /*
    Map<Theme, Double> emOutputs = themesRdd.collectAsMap();
    
    System.out.println(emOutputs.keySet().size() + " elements");
    int i = 0;
    for (Theme theme : emOutputs.keySet()) {
      Tuple2<Integer, Integer> t = theme.statistics();
      String themeLog = "Theme :" + i
              + "\n" + theme.sortTitleString(3)
              + "\n" + theme.sortString(12)
              + "\n" + "Score: " + emOutputs.get(theme)
              + "\n" + "Stats 1:" + t._1 + " / " + t._2
              + "\n" + "Stats 2:" + theme.statistics2()
              + "\n";
      
      System.out.println(themeLog);
      i += 1;
    }
    */
    int themeCount = (int) themesRdd.count();
    System.out.println("themesRdd = " + themeCount);
    
    System.out.println("KLDivergence starts");
    KLDivergence kldivergence = new KLDivergence(8., 1000.,themeCount);
    
    JavaRDD<LightTheme> themes = themesRdd.map(
            new Function<Tuple2<Theme,Double>,LightTheme>(){
            @Override
            public LightTheme call(Tuple2<Theme, Double> arg0) throws Exception {
              return new LightTheme(arg0._1());
            }
          }
    );
    JavaRDD<EvolutionaryTransition> transitionGraph = kldivergence.compute(themes);
    
    transitionGraph.cache();
    int transitionCount = (int) transitionGraph.count();
    System.out.println("themesRdd = " + themeCount);
    System.out.println("transitionGraph = " + transitionCount);

    System.out.println("KLDivergence done");

    int transitionCounter = 1;
    for (EvolutionaryTransition transition : transitionGraph.collect()) {
      System.out.println(transitionCounter++ + ". " + transition.toString());
    }
    System.out.println("themesRdd = " + themeCount);
    System.out.println("transitionGraph = " + transitionCount);
/*
    // generate the graph
    TimePeriod timePeriod = new TimePeriod(startDate, endDate);
    GraphVisualization.generateGraphFromRdd("graph.dot", timePeriod, themes, transitionGraph);
*/
  }

}
