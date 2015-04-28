package org.epfl.bigdataevs.executables;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.epfl.bigdataevs.em.EmAlgo;
import org.epfl.bigdataevs.em.EmInput;
import org.epfl.bigdataevs.em.Theme;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TextCollectionData;
import org.epfl.bigdataevs.eminput.TimePartition;
import org.epfl.bigdataevs.eminput.TimePeriod;

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
    
    TextCollectionData result = InputParser.getEmInput(timePeriods, ctx,inputPaths);
    
    
    
  }

}
