package org.epfl.bigdataevs.executables;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
import org.epfl.bigdataevs.evolutiongraph.EvolutionaryTransition;
import org.epfl.bigdataevs.evolutiongraph.KLDivergence;

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

public class EvolutionGraphTest {

  public static void main(String[] args) throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    System.out.println("STARTED TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    //sparkConf.setMaster("localhost:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");
    
    
    List<TimePeriod> timePeriods = new ArrayList<TimePeriod>(); 
    timePeriods.add(new TimePeriod(format.parse("1/2/1995-0"), format.parse("8/2/1995-0")));
    
    //System.out.println(timePeriods.get(0).includeDates(format.parse("1/1/1939-12")));
    
    List<String> inputPaths = new LinkedList<String>();
    inputPaths.add("hdfs:///projects/dh-shared/GDL/");
    //inputPaths.add("hdfs://user/christian/GDL");
    
    TextCollectionData result = InputParser.getEmInput(timePeriods, ctx,inputPaths);
    
    int numberOfRdd = (int) result.timePartitions.count();
    int numberOfDocuments = result.timePartitions.map(new Function<Tuple2<TimePeriod,TimePartition>, Integer>() {   
      @Override
      public Integer call(Tuple2<TimePeriod, TimePartition> v1) throws Exception {
        // TODO Auto-generated method stub
        return v1._2.parsedArticles.size();
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {

      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        // TODO Auto-generated method stub
        return v1+v2;
      }
    });
    
    int acc = 0;
    double threshold = Fraction.TWO.divide(new Fraction(result.backgroundModel.size())).doubleValue();
    for (String word : result.backgroundModel.keySet()) {
      if (word.matches("^-?\\d+$") && result.backgroundModel.get(word).doubleValue() < threshold) {
      //if (result.backgroundModel.get(word).doubleValue() < threshold) {
        acc += 1;      
      }
    }
    
    List<Tuple2<Integer, Integer>> wordsByInput = result.timePartitions.values().map(new Function<TimePartition, Tuple2<Integer, Integer>>() {
      @Override
      public Tuple2<Integer, Integer> call(TimePartition partition) throws Exception {
        List<String> allWords = new ArrayList<>();
        for (ParsedArticle article : partition.parsedArticles) {
          for (String word : article.words.keySet()) {
            if (!allWords.contains(word)) {
              allWords.add(word);
            }
          }
        }
        return new Tuple2(allWords.size(), partition.parsedArticles.size());
      }
    }).collect();
    
    System.out.println("Number of RDDs: " + numberOfRdd);
    System.out.println("Number of documents: " + numberOfDocuments);
    System.out.println("Number of words: " + result.backgroundModel.size());
    System.out.println("Number of words below threshold: " + acc);
    
    for (Tuple2<Integer, Integer> val : wordsByInput) {
      System.out.println("Number of distinct words in RDD:" + val._1 + ", number of articles: "+val._2);
    }
    
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
    
    int numberOfThemes = 20;
    double lambdaBackgroundModel = 0.5;
    int numberOfRuns = 1;
    EmAlgo emAlgo = new EmAlgo(ctx, result, numberOfThemes, lambdaBackgroundModel, numberOfRuns);
    
    emAlgo.run();
    JavaPairRDD<Theme, Double> themesRdd = emAlgo.relatedThemes();
    Map<Theme, Double> emOutputs = themesRdd.collectAsMap();
    
    int numberOfInputs = (int) emAlgo.selectedPartitions.count();
    int numberOfArticles = emAlgo.selectedPartitions.map(new Function<EmInput, Integer>() {
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
    
    List<Integer> numIters = emAlgo.selectedPartitions.map(new Function<EmInput, Integer>() {
      @Override
      public Integer call(EmInput input) throws Exception {
        return input.numberOfIterations;
      } 
    }).collect();
    
    List<Integer> numDocs = emAlgo.selectedPartitions.map(new Function<EmInput, Integer>() {
      @Override
      public Integer call(EmInput input) throws Exception {
        return input.documents.size();
      } 
    }).collect();
    
    List<List<Double>> maxLogLik = emAlgo.selectedPartitions.map(new Function<EmInput, List<Double>>() {
      @Override
      public List<Double> call(EmInput input) throws Exception {
        return input.values;
      } 
    }).collect();
    
    System.out.println(emOutputs.keySet().size() + " elements");
    int i = 0;
    for (Theme theme : emOutputs.keySet()) {
      Tuple2<Integer, Integer> t = theme.statistics();
      System.out.println("Theme :" + i);
      System.out.println(theme.sortString(12));
      System.out.println("Score:" + emOutputs.get(theme));
      System.out.println("Stats 1:" + t._1 + " / " + t._2);
      System.out.println("Stats 2:" + theme.statistics2());
      i += 1;
    }
    
    System.out.println("Number of EmInputs: " + numberOfInputs);
    System.out.println("Number of Articles: " + numberOfArticles);
    
    for (Integer val : numIters) {
      System.out.println("Number of iterations:" + val);
    }
    for (Integer val : numDocs) {
      System.out.println("Number of douments:" + val);
    }
    for (List<Double> values : maxLogLik) {
      System.out.println("Value of log-likelihood:");
      for (Double val : values) {
        System.out.println("   " + val);
      }
    }
   
    KLDivergence kldivergence = new KLDivergence(1E10, 0.0001);
  
    System.out.println("KLDivergence starts");
    
    JavaRDD<EvolutionaryTransition> transitionGraph = 
        kldivergence.compute(themesRdd.map(
                new Function<Tuple2<Theme,Double>,Theme>(){
                  @Override
                  public Theme call(Tuple2<Theme, Double> arg0) throws Exception {
                    return(arg0._1());
                  }
                  
                }
        ));

    System.out.println("KLDivergence done");
    
    System.out.println("themesRdd = "+themesRdd.count());
    System.out.println("transitionGraph = "+transitionGraph.count());
    
  }

}
/*

Theme :0
{de=0.036027465416267244, la=0.01863989530131822, le=0.013699287786830187, l=0.013694201371413998, d=0.013145765521409862, ?=0.012882480224206605, et=0.012118441672381484, les=0.011968877798697347, 2=0.011247653220150085, 5=0.010935372712445419, 1=0.010737900094285964, des=0.010150890060401981, en=0.008740461891966823}
Score:1.0
Stats 1:33001 / 33001
Stats 2:1.0000000000000018
Number of EmInputs: 1
Number of Articles: 835
Number of iterations:14
Number of douments:835
Value of log-likelihood:
   -10.47021106327416
   -9.637067028960864
   -8.973280036715154
   -8.461190040744457
   -8.124373606010323
   -7.940761554360895
   -7.852804626964587
   -7.813379862595895
   -7.796934603610762
   -7.790823148302712
   -7.788779215584215
   -7.78810954695479
   -7.787968414025956
   -7.788099982883336
KLDivergence starts
Exception in thread "main" org.apache.spark.SparkException: Task not serializable
  at org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:166)
  at org.apache.spark.util.ClosureCleaner$.clean(ClosureCleaner.scala:158)
  at org.apache.spark.SparkContext.clean(SparkContext.scala:1478)
  at org.apache.spark.rdd.RDD.flatMap(RDD.scala:295)
  at org.apache.spark.api.java.JavaRDDLike$class.flatMap(JavaRDDLike.scala:112)
  at org.apache.spark.api.java.JavaPairRDD.flatMap(JavaPairRDD.scala:45)
  at org.epfl.bigdataevs.evolutiongraph.KLDivergence.compute(KLDivergence.java:33)
  at org.epfl.bigdataevs.executables.EvolutionGraphTest.main(EvolutionGraphTest.java:206)
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:606)
  at org.apache.spark.deploy.SparkSubmit$.launch(SparkSubmit.scala:358)
  at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:75)
  at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Caused by: java.io.NotSerializableException: org.epfl.bigdataevs.evolutiongraph.KLDivergence
  at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1183)
  at java.io.ObjectOutputStream.defaultWriteFields(ObjectOutputStream.java:1547)
  at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1508)
  at java.io.ObjectOutputStream.writeOrdinaryObject(ObjectOutputStream.java:1431)
  at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1177)
  at java.io.ObjectOutputStream.defaultWriteFields(ObjectOutputStream.java:1547)
  at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1508)
  at java.io.ObjectOutputStream.writeOrdinaryObject(ObjectOutputStream.java:1431)
  at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1177)
  at java.io.ObjectOutputStream.writeObject(ObjectOutputStream.java:347)
  at org.apache.spark.serializer.JavaSerializationStream.writeObject(JavaSerializer.scala:42)
  at org.apache.spark.serializer.JavaSerializerInstance.serialize(JavaSerializer.scala:73)
  at org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:164)


*/