package exec;

import java.util.ArrayList;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.epfl.bigdataevs.eminput.ArticleProcessor;
import org.epfl.bigdataevs.eminput.ArticleStream;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.RawArticle;
import org.epfl.bigdataevs.eminput.TimePartition;
import org.epfl.bigdataevs.eminput.TimePeriod;

public class ArticleProcessorTest {

  public static void main(String[] args) {
    System.out.println("STARTING UP TEST");
    
    SparkConf sparkConf = new SparkConf().setAppName("Test article processor");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    String testText1 = "I am a computer scientist, and currently I'm programming"
        + " a Big Data assignment like 99% of the time.";
    String testText2 = "This time period is tough for a scientist, I'm considering"
        + " to study in other big fields.";
    
    
    RawArticle testInput1 = new RawArticle(testText1.length(), 1, 18, testText1, "0",
        new Date(System.currentTimeMillis() - 100000), "Test text 1", ArticleStream.GDL);
    RawArticle testInput2 = new RawArticle(testText1.length(), 1, 15, testText2, "1",
        new Date(System.currentTimeMillis()), "Test text 2", ArticleStream.JDG);
    
    System.out.println("Turning test data into RDD");
    ArrayList<RawArticle> data = new ArrayList<RawArticle>();
    data.add(0, testInput1);
    data.add(1, testInput2);
    JavaRDD<RawArticle> rawDocs = ctx.parallelize(data);
    
    System.out.println("Processing RDD...");
    TimePartition result = ArticleProcessor
            .generateTimePartitionModel(rawDocs, new TimePeriod(testInput1.issueDate, testInput2.issueDate));
    
    System.out.println("======Background model's content======");
    for(String background_word : result.backgroundModel.keySet()) {
      System.out.println(background_word + ": " 
        + result.backgroundModel.get(background_word) + " distribution proba.");
    }
    
    System.out.println("=======Obtained " + result.parsedArticles.size() + " parsed articles====");
    int count = 1;
    for (ParsedArticle output : result.parsedArticles) {
      System.out.println("Article " + count + ":");
      System.out.println("Assigned stream is " + output.stream + ". Individual word count:");
      for (String word: output.words.keySet()) {
        System.out.println(word + ": " + output.words.get(word) + " occurrences.");
      }
    }

  }

}
