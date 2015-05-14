package org.epfl.bigdataevs.executables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.epfl.bigdataevs.em.EmAlgo;
import org.epfl.bigdataevs.em.Theme;
import org.epfl.bigdataevs.eminput.EmInputFromParser;
import org.epfl.bigdataevs.eminput.HmmInputFromParser;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.TimePartition;
import org.epfl.bigdataevs.eminput.TimePeriod;
import org.epfl.bigdataevs.evolutiongraph.EvolutionaryTransition;
import org.epfl.bigdataevs.evolutiongraph.KLDivergence;
import org.epfl.bigdataevs.hmm.LifeCycleAnalyserSpark;

import scala.Tuple2;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

public class ThemesStrengthOverTime {

  public static void main(String[] args) throws NumberFormatException, XMLStreamException, ParseException, IOException {
    System.out.println("STARTED TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Themes strengths over time");
    //sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    
    
    Parameters.parseParameters("conf.txt");
       
    
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>();
    

    Calendar c = Calendar.getInstance();
    Date startDate = format.parse(Parameters.startDate);
    c.setTime(startDate);
    for (int i = 0; i < Parameters.dateStepsNumber; i++) {
      Date c1 = c.getTime();
      c.add(Calendar.DATE, Parameters.dateStepSize);
      Date c2 = c.getTime();
      timePeriods.add(new TimePeriod(c1, c2));
      System.out.println(c1 + "-" + c2);
    }
    Date endDate = c.getTime();
    
    Calendar cHmm = Calendar.getInstance();
    Date startDateHmm = format.parse(Parameters.startDateHMM);
    cHmm.setTime(startDateHmm);
    Date beginning = cHmm.getTime();
    cHmm.add(Calendar.DATE, Parameters.dateStepSizeHMM);
    Date end = cHmm.getTime();
    TimePeriod timePeriodHmm = new TimePeriod(beginning, end);
    System.out.println(beginning + "-" + end);
            
            
            
    /*
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>(); 
    timePeriods.add(new TimePeriod(format.parse("1/10/1962-0"), format.parse("14/10/1962-0")));
    timePeriods.add(new TimePeriod(format.parse("15/10/1962-0"), format.parse("30/10/1962-0")));
    timePeriods.add(new TimePeriod(format.parse("31/10/1962-0"), format.parse("14/11/1962-0")));
    timePeriods.add(new TimePeriod(format.parse("15/11/1962-0"), format.parse("30/11/1962-0")));
    timePeriods.add(new TimePeriod(format.parse("01/12/1962-0"), format.parse("14/12/1962-0")));
    timePeriods.add(new TimePeriod(format.parse("15/12/1962-0"), format.parse("31/12/1962-0")));
    timePeriods.add(new TimePeriod(format.parse("01/01/1963-0"), format.parse("15/01/1963-0")));
    timePeriods.add(new TimePeriod(format.parse("16/01/1963-0"), format.parse("31/01/1963-0")));
    timePeriods.add(new TimePeriod(format.parse("01/02/1963-0"), format.parse("15/02/1963-0")));
    timePeriods.add(new TimePeriod(format.parse("16/02/1963-0"), format.parse("28/02/1963-0")));
    timePeriods.add(new TimePeriod(format.parse("01/03/1963-0"), format.parse("15/03/1963-0")));
    timePeriods.add(new TimePeriod(format.parse("06/03/1963-0"), format.parse("31/03/1963-0")));
    */
    
    
    //System.out.println(timePeriods.get(0).includeDates(format.parse("1/1/1939-12")));
    
    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs:///projects/dh-shared/GDL/");
    inputPaths.add("hdfs:///projects/dh-shared/JDG/");
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

    
    InputParser parserEM = new InputParser(TimePeriod.getEnglobingTimePeriod(timePeriods), timePeriodHmm, 
            ctx, inputPaths, Parameters.numberOfCountsBackgroundModelThreshold,
            Parameters.firstNumberOfPagesInNewspaperThreshold);
    InputParser parserHMM = new InputParser(timePeriodHmm, 
            ctx, inputPaths);
    EmInputFromParser emInputFromParser = parserEM.getEmInput(timePeriods);
    HmmInputFromParser hmmInputFromParser = parserHMM.getHmmInput();
    
    List<Integer> numArticles = emInputFromParser.timePartitions.
            map(new Function<Tuple2<TimePeriod,TimePartition>, Integer>() {

      @Override
      public Integer call(Tuple2<TimePeriod, TimePartition> v1) throws Exception {
        // TODO Auto-generated method stub
        return v1._2.parsedArticles.size();
      }
      
    }).collect();
    for (Integer integer : numArticles) {
      System.out.println("Number of articles : " + integer);
    }
    
    EmAlgo emAlgo = new EmAlgo(ctx, emInputFromParser);
    
    JavaPairRDD<Theme, Double> themesRdd = emAlgo.run();   
    themesRdd.cache();
    

   /*
    KLDivergence kldivergence = new KLDivergence(42., 100.);
  
    System.out.println("KLDivergence starts");
    
    JavaRDD<EvolutionaryTransition> transitionGraph = 
        kldivergence.compute(themesRdd.map(
                new Function<Tuple2<Theme,Double>,Theme>(){
                @Override
                public Theme call(Tuple2<Theme, Double> arg0) throws Exception {
                  return (arg0._1());
                }
              }
        ));

    System.out.println("KLDivergence done");
    
    System.out.println("themesRdd = " + themesRdd.count());
    System.out.println("transitionGraph = " + transitionGraph.count());
    int transitionCount = 1;
    for (EvolutionaryTransition transition : transitionGraph.collect()) {
      System.out.println(transitionCount++ + ". " + transition.toString());
    }
    */
    
    
    //hmm  begins
    final double piThreshold = 0.01;
    final double aaThreshold = 0.01;
    final int maxIterations = 50;
    
    System.out.println("Beginning life cycle analysis");
    System.out.println("Printing inputs");
    long sequenceLength = hmmInputFromParser.wordStream.count();
    System.out.println("sequence length : " + sequenceLength);
    System.out.println("wordStream : " 
            + Arrays.toString(Arrays.copyOf(hmmInputFromParser.wordStream.collect().toArray(),50)));
    LifeCycleAnalyserSpark lifeCycleAnalyser = new LifeCycleAnalyserSpark(hmmInputFromParser);
    lifeCycleAnalyser.addAllThemesFromRdd(themesRdd);
    lifeCycleAnalyser.analyse(ctx, piThreshold, aaThreshold, maxIterations);
    System.out.println("DecodedStream : "
            + Arrays.toString(Arrays.copyOf(lifeCycleAnalyser.mostLikelySequenceThemeShifts.collect().toArray(),50)));
    
    JavaRDD<Tuple2<Integer,Integer>> nonZeroMostLikely = 
            lifeCycleAnalyser.mostLikelySequenceThemeShifts
            .flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, 
                    Tuple2<Integer, Integer>>(){

                      @Override
                      public Iterable<Tuple2<Integer, Integer>> call(Tuple2<Integer, Integer> arg0)
                              throws Exception {
                       ArrayList<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>(1);
                         if(arg0._2 != 0){
                           list.add(arg0);
                         }
                       return list;
                      }
              
            });
    System.out.println("DecodedStream non zero: "+Arrays.toString(Arrays.copyOf(nonZeroMostLikely.collect().toArray(),50)));
    System.out.println(nonZeroMostLikely.count()+" detected non-zero states over "+sequenceLength+" states");
    //  Print A then
    double[][] trainedA = lifeCycleAnalyser.hmm.getA();
    int N = lifeCycleAnalyser.hmm.getN();
    System.out.println("N "+N);
    System.out.println("A: ");
    for ( int k = 0; k < N; k++ ) {
      for (int j = 0; j < N; j++ ) {
        System.out.print(" " + trainedA[k][j]);
      }
      System.out.println("");
    }
    
    System.out.println("Strength of theme 1 at time 30 (window 20) :"+lifeCycleAnalyser.absoluteStrength(1, 30, 20));
    
    
    
    
  }

}
