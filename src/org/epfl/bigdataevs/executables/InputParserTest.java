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
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

public class InputParserTest {
  
  /* Number of time periods in our time frame. A time period is one month long*/
  public static final int NUM_PARTITIONS = 60;
  public static final long YEAR_START = 1939;

  public static void main(String[] args) throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    System.out.println("STARTED TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    //sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    Calendar c = Calendar.getInstance(); 
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>();
    /*Build our time frame by iteratively adding time periods of one month length*/
    Date start = format.parse("2/1/" + YEAR_START + "-11");
    Date end = format.parse("1/2/" + YEAR_START + "-13");
    for (int i=0; i<NUM_PARTITIONS; i++) {
      timePeriods.add(new TimePeriod(start, end));
      c.setTime(start); 
      c.add(Calendar.MONTH, 1);
      start = c.getTime();
      c.setTime(end); 
      c.add(Calendar.MONTH, 1);
      end = c.getTime();
    }

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
