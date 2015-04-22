package org.epfl.bigdataevs.executables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.epfl.bigdataevs.eminput.RawArticle;
import org.epfl.bigdataevs.eminput.RawArticleInputStream;
import org.epfl.bigdataevs.eminput.TimePeriod;

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
    Calendar cal = Calendar.getInstance();
    cal.set(1939, 0, 1,0,0,0);
    Date begin = cal.getTime();
    cal.set(1945, 11, 31,13,0,0);
    Date end = cal.getTime();  
    end.getDate();
    
    System.out.println("Articles from : " + begin + " to : " + end);
    TimePeriod period = new TimePeriod(begin, end);
    
    Date date = format.parse("4/1/1939-12");
    System.out.println(date);
    System.out.println(period.includeDates(date));
    
    System.out.println("Files :");
    for (String name:period.getFilesNames()) {
      System.out.println(name);
    }
    
    List<String> folder = new LinkedList<String>();
    //folder.add("file:///Users/christian/dhlabProject/JDG");
    //folder.add("/Users/christian/dhlabProject/GDL");
    folder.add("hdfs://localhost:9000/user/christian/JDG");
    
    Configuration config = new Configuration();
    config.addResource(new Path("/usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/core-site.xml"));
    //config.addResource(new Path("/usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/hdfs-site.xml"));
    
    RawArticleInputStream ras = new RawArticleInputStream(period, folder, config);
    
    RawArticle art;
    int count = 0;
    int LIMIT = 1000000;
    
    List<RawArticle> artl = new LinkedList<RawArticle>();
    while ((art = ras.read()) != null && count++ < LIMIT) {
      artl.add(art);
      System.out.println(count + " - " + art.issueDate.toString());
    }
    System.out.println("Done");
    System.out.println(artl.size());
  }
}
