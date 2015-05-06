package org.epfl.bigdataevs.evolutiongraph;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.epfl.bigdataevs.em.Theme;

import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

public class GraphVisualization {
  
  private static void generateGraphFromString(String filename, List<Tuple2<Date,List<String>>> nodes, List<Tuple2<String,String>> transitions){
    PrintWriter writer = null;

    try {
      writer = new PrintWriter(filename);
    } catch (FileNotFoundException e){
      System.out.println("Couldn't create file");
      return;
    }
    
    writer.println("digraph EvolutionGraph {\n\tranksep=.75; size = \"7.5,7.5\";\n\t{");
    writer.println("\t\tnode [shape=plaintext, fontsize=16];");
    
    /* Write dates */
    for(Tuple2<Date, List<String>> node : nodes){
      Date date = node._1();
      writer.println("\t\t"+date.toString()+" ->");
    }
    writer.println("\t\t\"today\";");

    writer.println("\t}\n\t{\n\t\tnode [shape=box];");     
    
    /* Write themes */
    for(Tuple2<Date, List<String>> node : nodes){
      Date date = node._1();
      List<String> themes = node._2();
      
      writer.print("\t\t{ rank = same; ");
      writer.print("\t\t\"" + date.toString() + "\" ; \n");
      
      for(String theme : themes){
        writer.print(" \""+theme+"\";");
      }
      
      writer.println("}");
    }
    
    /* Write transitions */
    for(Tuple2<String,String> transition : transitions){
      String th1 = transition._1();
      String th2 = transition._2();
      
      writer.println("\t\t\""+th1+"\" -> \""+th2+"\"");
    }
    
    writer.println("\t}\n}");
    
    writer.close();
  }
  
  public static void generateGraphFromRdd(JavaRDD<Theme> themesRdd, JavaRDD<EvolutionaryTransition> transitionGraph){
    themesRdd.map(new Function<Theme,Tuple2<Date,String>>(){
      @Override
      public Tuple2<Date, String> call(Theme theme) throws Exception {
        
        return null;
      }
    });
    
    transitionGraph.map(new Function<EvolutionaryTransition, Tuple2<String,String>>(){
      @Override
      public Tuple2<String, String> call(EvolutionaryTransition transition) throws Exception {
        transition.theme1.toString();
        transition.theme2.toString();
        return new Tuple2(transition.theme1.toString(), transition.theme2.toString());
      }
      
      
    });
  }
}
