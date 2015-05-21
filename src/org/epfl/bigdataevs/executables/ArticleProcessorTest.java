package org.epfl.bigdataevs.executables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.epfl.bigdataevs.input.ArticleStream;
import org.epfl.bigdataevs.input.RawArticle;
import org.epfl.bigdataevs.input.TimePeriod;

import java.util.ArrayList;
import java.util.Date;

public class ArticleProcessorTest {

  public static void main(String[] args) {
    System.out.println("STARTING UP TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    /*Test set-up: 3 articles over two time partitions
     * text 1 belongs to partition 1, text 3 to partition 2, and text 2
     * to both partitions (overlap)
     * 
     * This makes sure we don't count twice the words in overlapping partitions
     * in the background model*/
    String testText1 = "word123 word1 word13";
    String testText2 = "word123 word2 word23";
    String testText3 = "word123 word3 word23 word13";
    
    long now = System.currentTimeMillis();
    Date issueDate1 = new Date(now);
    Date issueDate2 = new Date(now + 100000);
    Date issueDate3 = new Date(now + 200000);
    
    //contains issueDate1 and issueDate2
    TimePeriod partition1 = new TimePeriod(new Date(now - 1000), new Date(now + 150000));
    //contains issueDate2 and issueDate3
    TimePeriod partition2 = new TimePeriod(new Date(now + 50000), new Date(now + 250000));
    
    ArrayList<TimePeriod> partitions = new ArrayList<TimePeriod>(2);
    partitions.add(0, partition1);
    partitions.add(1, partition2);
    
    RawArticle testInput1 = new RawArticle(1, testText1, "0",
        issueDate1, "Test text 1", ArticleStream.GDL);
    RawArticle testInput2 = new RawArticle(1, testText2, "1",
        issueDate2, "Test text 2", ArticleStream.JDG);
    RawArticle testInput3 = new RawArticle(1, testText3, "2",
            issueDate3, "Test text 3", ArticleStream.JDG);
    
    System.out.println("Turning test data into RDD");
    ArrayList<RawArticle> data = new ArrayList<RawArticle>();
    data.add(0, testInput1);
    data.add(1, testInput2);
    data.add(2, testInput3);
    JavaRDD<RawArticle> rawDocs = ctx.parallelize(data);
    /*
    System.out.println("Processing RDD...");
    TextCollectionData result = TextCollectionData
            .generateTextCollectionData(rawDocs, partitions);
    
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
    /*System.out.println("=======Obtained " + result.parsedArticles.size() + " parsed articles====");
    int count = 1;
    for (ParsedArticle output : result.parsedArticles) {
      System.out.println("Article " + count + ":");
      System.out.println("Assigned stream is " + output.stream + ". Individual word count:");
      for (String word: output.words.keySet()) {
        System.out.println(word + ": " + output.words.get(word) + " occurrences.");
      }
    }*/
    
    ctx.close();

  }

}
