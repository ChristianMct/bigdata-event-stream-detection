package org.epfl.bigdataevs.executables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.epfl.bigdataevs.eminput.HmmInputFromParser;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.TimePeriod;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

public class InputParserTest {

  public static void main(String[] args) throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    System.out.println("STARTED TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    
    
    sparkConf.set("spark.executor.memory","2g");
    sparkConf.set("spark.driver.memory", "2g");
    //sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    
    
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>(); 
    timePeriods.add(new TimePeriod(format.parse("1/1/1939-11"), format.parse("1/2/1945-13")));
    
    //System.out.println(timePeriods.get(0).includeDates(format.parse("1/1/1939-12")));
    
    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs:///projects/dh-shared/GDL/");
    //inputPaths.add("hdfs://user/christian/GDL");
    
    InputParser parser = new InputParser(TimePeriod.getEnglobingTimePeriod(timePeriods), ctx, inputPaths);
    
    HmmInputFromParser result = parser.getHmmInput();

    long bgsize = result.backgroundModel.backgroundModelRdd.count();
    long wordstrSize = result.wordStream.count();
    long lexiconsize = result.lexicon.count();
    
    ctx.close();
    
    System.out.println("Size of BG: "+bgsize);   
    System.out.println("Size of wordStream: "+wordstrSize);
    System.out.println("Size of lexicon: "+lexiconsize);
  }

}
