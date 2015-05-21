package org.epfl.bigdataevs.executables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.epfl.bigdataevs.input.RawArticle;
import org.epfl.bigdataevs.input.RawArticleInputStream;
import org.epfl.bigdataevs.input.TimePeriod;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

public class RawArticleStreamTest{
  
  public static void main(String[] args) 
          throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy-HH");

    Date begin = format.parse("1/1/1939-11");
    Date end = format.parse("31/12/1939-13");
    
    System.out.println("Articles from : " + begin + " to : " + end);
    TimePeriod period = new TimePeriod(begin, end);
    
    
    System.out.println("Files :");
    for (String name:period.getFilesNames()) {
      System.out.println(name);
    }
    
    
    //folder.add("file:///Users/christian/dhlabProject/JDG");
    //folder.add("/Users/christian/dhlabProject/GDL");
    String path = "hdfs://localhost:9000/user/christian/JDG/articles1939.xml";
    
    Configuration config = new Configuration();
    //config.addResource(new Path("/usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/core-site.xml"));
    //config.addResource(new Path("/usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/hdfs-site.xml"));
    
    RawArticleInputStream ras = new RawArticleInputStream(period, path);
    
    RawArticle art;
    int count = 0;
    int LIMIT = 1000000;
    
    List<RawArticle> artl = new LinkedList<RawArticle>();
    while ((art = ras.read()) != null && count++ < LIMIT) {
      if(true) {
        artl.add(art);
        //System.out.println(count + " - " + art.pageNumber);
        System.out.println("Name: "+art.name);
        //System.out.println("Full text: "+art.fullText);
        System.out.println(art.fullText);
        System.out.println("-------------------------------------------------------------------------------------------------------");
      }
    }
    System.out.println("Done");
    System.out.println(artl.size());
  }
}
