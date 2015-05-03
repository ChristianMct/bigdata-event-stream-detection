package org.epfl.bigdataevs.executables;

import org.apache.commons.math3.fraction.BigFraction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.epfl.bigdataevs.eminput.BackgroundModel;
import org.epfl.bigdataevs.eminput.HmmInputFromParser;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.TimePeriod;

import scala.Tuple2;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.stream.XMLStreamException;

public class BackgroundModelTest {

  public static void main(String[] args) throws ParseException, NumberFormatException, XMLStreamException, IOException {
    System.out.println("STARTED TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    
    sparkConf.set("spark.executor.memory","2g");
    sparkConf.set("spark.driver.memory", "2g");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs:///projects/dh-shared/GDL/");
    inputPaths.add("hdfs:///projects/dh-shared/JDG/");
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    Date from = format.parse("1/1/1930-11");
    Date to = format.parse("31/12/1970-13");
    TimePeriod timePeriod = new TimePeriod(from, to);
    
    InputParser parser = new InputParser(timePeriod, ctx, inputPaths,1);
    
    BackgroundModel result = parser.getBackgroundModel(7);
    
    JavaPairRDD<Long,Long> counts = result.backgroundModelRdd
            .mapToPair(new PairFunction<Tuple2<String,BigFraction>,Long, Long>() {
              @Override
              public Tuple2<Long, Long> call(Tuple2<String, BigFraction> t)
                      throws Exception {
                return new Tuple2<Long, Long>(t._2.getNumeratorAsLong(), (long) 1);
              }
            });
    
    // <Count, NumberOfWordHavingCOunt>
    JavaPairRDD<Long,Long> histCount = counts.reduceByKey(new Function2<Long,Long,Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1+v2;
      }
    }).sortByKey();
    
    
    
    Map<Long,Long> histMap = histCount.collectAsMap();
    
    ctx.close();
    
    for (long i=1; i<histMap.size(); i++) {
      if (histMap.containsKey(new Long(i)))
        System.out.println(i+" => "+histMap.get(new Long(i)));
    }
    
  }
  
}
