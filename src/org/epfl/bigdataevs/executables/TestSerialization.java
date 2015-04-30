package org.epfl.bigdataevs.executables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.epfl.bigdataevs.eminput.InputParser;
import org.epfl.bigdataevs.eminput.RawArticleInputStream;
import org.epfl.bigdataevs.eminput.TimePeriod;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

public class TestSerialization {

  public static void main(String[] args) throws ParseException, IOException, NumberFormatException, XMLStreamException {
    
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    Configuration config = new Configuration();
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    
    TimePeriod timePeriod = new TimePeriod(format.parse("1/1/1939-11"), format.parse("1/1/1945-13"));
    
    String path = "hdfs://localhost:9000/user/christian/JDG/articles1939.xml";
    
    List<String> sourcePath = new LinkedList<String>();
    sourcePath.add("hdfs://localhost:9000/user/christian/JDG/");
    
    new ObjectOutputStream(
            new ByteArrayOutputStream()
            ).writeObject(new RawArticleInputStream(timePeriod, path));
    
    
    

    new ObjectOutputStream(
            new ByteArrayOutputStream()
            ).writeObject(new InputParser(timePeriod, ctx, sourcePath));
    
    System.out.println("Success !");
  }

}
