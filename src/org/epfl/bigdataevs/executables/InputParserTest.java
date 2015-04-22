package org.epfl.bigdataevs.executables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.TextCollectionData;
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

  public static void main(String[] args) throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    System.out.println("STARTED TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    //sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    
    
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>(); 
    timePeriods.add(new TimePeriod(format.parse("1/1/1939-11"), format.parse("2/1/1939-13")));
    timePeriods.add(new TimePeriod(format.parse("2/1/1939-11"), format.parse("3/1/1939-13")));
    
    System.out.println(timePeriods.get(0).includeDates(format.parse("1/1/1939-12")));
    
    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs://user/christian/JDG");
    inputPaths.add("hdfs://user/christian/GDL");
    
    TextCollectionData result = InputParser.getEmInput(timePeriods, ctx, "hdfs://user/christian/JDG");
    
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
  }

}
