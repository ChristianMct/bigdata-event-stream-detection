package org.epfl.bigdataevs.executables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.epfl.bigdataevs.em.EmAlgo;
import org.epfl.bigdataevs.em.EmInput;
import org.epfl.bigdataevs.em.Theme;
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
    timePeriods.add(new TimePeriod(format.parse("1/2/1995-11"), format.parse("2/2/1995-13")));
    //timePeriods.add(new TimePeriod(format.parse("2/1/1939-11"), format.parse("3/1/1939-13")));
    
    System.out.println(timePeriods.get(0).includeDates(format.parse("1/1/1939-12")));
    
    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs:///projects/dh-shared/GDL/");
    //inputPaths.add("hdfs://user/christian/GDL");
    
    TextCollectionData result = InputParser.getEmInput(timePeriods, ctx,inputPaths);
    
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
    int numberOfThemes = 10;
    double lambdaBackgroundModel = 0.9;
    int numberOfRuns = 1;
    EmAlgo emAlgo = new EmAlgo(ctx, result, numberOfThemes, lambdaBackgroundModel, numberOfRuns);
    
    JavaRDD<EmInput> selectedInputs = emAlgo.run();
    Map<Theme, Double> emOutputs = emAlgo.relatedThemes(selectedInputs).collectAsMap();
    
    int numberOfInputs = (int) selectedInputs.count();
    int numberOfArticles = selectedInputs.map(new Function<EmInput, Integer>() {
      @Override
      public Integer call(EmInput input) throws Exception {
        return input.documents.size();
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {
      
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        // TODO Auto-generated method stub
        return v1+v2;
      }
    });
    
    List<Integer> numIters = selectedInputs.map(new Function<EmInput, Integer>() {
      @Override
      public Integer call(EmInput input) throws Exception {
        return input.numberOfIterations;
      } 
    }).collect();
    
    System.out.println(emOutputs.keySet().size() + " elements");
    int i = 0;
    for (Theme theme : emOutputs.keySet()) {
      System.out.println("Theme :" + i);
      System.out.println(theme.sortString(12));
      System.out.println("Score:" + emOutputs.get(theme));
      System.out.println("Stats 1:" + theme.statistics());
      System.out.println("Stats 2:" + theme.statistics2());
      i += 1;
    }
    
    System.out.println("Number of EmInputs: " + numberOfInputs);
    System.out.println("Number of Articles: " + numberOfArticles);
    
    for (Integer val : numIters) {
      System.out.println("Number of iterations:" + val);
    }
  }

}
