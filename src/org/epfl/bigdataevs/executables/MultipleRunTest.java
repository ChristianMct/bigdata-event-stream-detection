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
import org.epfl.bigdataevs.em.ThemeFromLargeTimePeriod;
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
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;

public class MultipleRunTest {

  public static void main(String[] args) throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    System.out.println("STARTED TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    //sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    
    
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>();    
    Calendar cb = Calendar.getInstance();
    cb.setTime(format.parse("01/01/1978-0"));
    Date cb1 = cb.getTime();
    cb.add(Calendar.DATE, 365);
    Date cb2 = cb.getTime();
    TimePeriod backgroundModelTimePeriod = new TimePeriod(cb1, cb2);    
    
    System.out.println("Background model time period: " + cb1 + " - " + cb2);
    
    List<Tuple2<String, Integer>> timePeriodsDescriptions = new ArrayList<Tuple2<String,Integer>>();
    timePeriodsDescriptions.add(new Tuple2<String, Integer>("7/11/1989-0", 7));
    timePeriodsDescriptions.add(new Tuple2<String, Integer>("15/11/1989-0", 7));
    
    cb.setTime(format.parse("01/01/1978-0"));
    for(int i=0; i<50; i++){
      Date c1 = cb.getTime();
      cb.add(Calendar.DATE, 7);
      Date c2 = cb.getTime();
      timePeriods.add(new TimePeriod(c1, c2));
      System.out.println(c1 + " - " + c2);
    }
    
    List<String> targetWords = new ArrayList<>();
    /*
    targetWords.add("Spoutnik");
    targetWords.add("satellite");
    targetWords.add("soviétique");
    targetWords.add("orbite");
    targetWords.add("URSS");

    targetWords.add("homme");
    targetWords.add("premier");
    targetWords.add("astronautes");
    targetWords.add("lunaire");
    targetWords.add("apollo");
    targetWords.add("lune");
    targetWords.add("spatial");    
    
    for (Tuple2<String, Integer> tuple : timePeriodsDescriptions) {
      Calendar c = Calendar.getInstance();
      c.setTime(format.parse(tuple._1));
      //for(int i=0; i<2; i++){
      Date c1 = c.getTime();
      c.add(Calendar.DATE, tuple._2);
      Date c2 = c.getTime();
      timePeriods.add(new TimePeriod(c1, c2));
      System.out.println(c1 + " - " + c2);
      //}
    }
    
    */
    
    //System.out.println(timePeriods.get(0).includeDates(format.parse("1/1/1939-12")));
    
    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs:///projects/dh-shared/GDL/");
    inputPaths.add("hdfs:///projects/dh-shared/JDG/");
 
    
    /*
     * Integration of the EM Algorithm
     */
    
    int wordsThreshold = Parameters.numberOfCountsBackgroundModelThreshold;
    int pageThreshold = Parameters.firstNumberOfPagesInNewspaperThreshold;
    
    InputParser parser = new InputParser(TimePeriod.getEnglobingTimePeriod(timePeriods),
            backgroundModelTimePeriod, ctx, inputPaths,
            wordsThreshold, pageThreshold);
    EmInputFromParser emInputFromParser = parser.getEmInput(timePeriods);
    
    List<Integer> numArticles = emInputFromParser.timePartitions.map(
            new Function<Tuple2<TimePeriod,TimePartition>, Integer>() {

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
    
    Map<Theme, Double> emOutputs = themesRdd.collectAsMap();
    
    System.out.println(emOutputs.keySet().size() + " elements");
    int i = 0;
    for (Theme theme : emOutputs.keySet()) {
      TreeMap<String, Double> sortedWords = theme.sortString(12);
      String themeLog = "Theme :" + i
              + "\n" + theme.timePeriod.from + " - " + theme.timePeriod.to
              + "\n" + theme.sortTitleString(10)
              + "\n" + sortedWords
              + "\n" + "Score: " + emOutputs.get(theme)
              + "\n";
      
      System.out.println(themeLog);
      for (String target : targetWords) {
        if (theme.wordsProbability.keySet().contains(target)) {
          System.out.println("Contains word " + target + "(" + theme.wordsProbability.get(target) + ")");
        }
      }
      i += 1;
    }
      
  }

}
