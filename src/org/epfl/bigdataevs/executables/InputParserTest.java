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
    //sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    
    
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>(); 
    timePeriods.add(new TimePeriod(format.parse("1/1/1939-11"), format.parse("1/1/1940-13")));
    
    //System.out.println(timePeriods.get(0).includeDates(format.parse("1/1/1939-12")));
    
    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs:///projects/dh-shared/GDL/");
    //inputPaths.add("hdfs://user/christian/GDL");
    
    InputParser parser = new InputParser(TimePeriod.getEnglobingTimePeriod(timePeriods), ctx, inputPaths);
    
    HmmInputFromParser result = parser.getHmmInput(null);
    
    System.out.println("Size of BG model: "+result.backgroundModel.backgroundModelRdd.count());
    System.out.println("Size of wordStream: "+result.wordStream.count());
    System.out.println("Size of lexicon: "+result.lexicon.count());
    
  }

}
