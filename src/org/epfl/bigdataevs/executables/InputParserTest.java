package org.epfl.bigdataevs.executables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.epfl.bigdataevs.em.EmAlgo;
import org.epfl.bigdataevs.em.Theme;
import org.epfl.bigdataevs.input.EmInputFromParser;
import org.epfl.bigdataevs.input.HmmInputFromParser;
import org.epfl.bigdataevs.input.InputParser;
import org.epfl.bigdataevs.input.TimePeriod;

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

public class InputParserTest {
  
  /* Number of time periods in our time frame. A time period is one month long*/
  public static final int NUM_PARTITIONS = 51*4;
  public static final long YEAR_START = 1990;

  public static void main(String[] args) throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    System.out.println("STARTED TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    
    
    sparkConf.set("spark.executor.memory","2g");
    sparkConf.set("spark.driver.memory", "2g");
    //sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    Calendar c = Calendar.getInstance(); 
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>();
    /*Build our time frame by iteratively adding time periods of one month length*/
    Date start = format.parse("1/1/" + YEAR_START + "-11");
    Date end = format.parse("7/2/" + YEAR_START + "-13");
    for (int i=0; i<NUM_PARTITIONS; i++) {
      timePeriods.add(new TimePeriod(start, end));
      c.setTime(start); 
      c.add(Calendar.WEEK_OF_MONTH, 1);
      start = c.getTime();
      c.setTime(end); 
      c.add(Calendar.WEEK_OF_MONTH, 1);
      end = c.getTime();
    }
    
    

    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs:///projects/dh-shared/GDL/");
    //inputPaths.add("hdfs://user/christian/GDL");
    
    InputParser parser = new InputParser(TimePeriod.getEnglobingTimePeriod(timePeriods), ctx, inputPaths);
    
//    HmmInputFromParser result = parser.getHmmInput();
//
//    long bgsize = result.backgroundModel.backgroundModelRdd.count();
//    long wordstrSize = result.wordStream.count();
//    long lexiconsize = result.lexicon.count();
//    
//    ctx.close();
//    
//    System.out.println("Size of BG: "+bgsize);   
//    System.out.println("Size of wordStream: "+wordstrSize);
//    System.out.println("Size of lexicon: "+lexiconsize);
    
    int numThemes = 10;
    double lambda = 0.8;
    int numRuns = 1;
    
    EmInputFromParser emInputFromParser = parser.getEmInput(timePeriods);
    
    long bgSize = emInputFromParser.backgroundModel.backgroundModelRdd.count();
    long tpSize = emInputFromParser.timePartitions.count();
    
    ctx.close();
    
    System.out.println("Done, BG size : "+bgSize+" tp size : "+tpSize);
    
//    EmAlgo emAlgo = new EmAlgo(ctx, emInputFromParser, numThemes, lambda, numRuns);
//    
//    Map<Theme, Double> output = emAlgo.run().collectAsMap();
//    int counter = 0;
//    System.out.println("Number of themes: " + output.size());
//    for (Theme theme : output.keySet()) {
//      System.out.println("Theme " + counter);
//      counter += 1;
//      String s = "";
//      TreeMap<String, Double> bestWords = theme.sortString(12);
//      for (String string : bestWords.keySet()) {
//        s += string+" (" + bestWords.get(string) + "), ";
//      }
//      System.out.println(s);
//    }
  }

}
