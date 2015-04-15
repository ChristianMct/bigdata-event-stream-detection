package exec;

import org.apache.hadoop.conf.Configuration;
import org.epfl.bigdataevs.eminput.RawArticle;
import org.epfl.bigdataevs.eminput.RawArticleInputStream;
import org.epfl.bigdataevs.eminput.TimePeriod;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

public class RawArticleStreamTest {
  
  public static void main(String[] args) throws NumberFormatException, XMLStreamException, ParseException, IOException {
    
    Calendar cal = Calendar.getInstance();
    cal.set(1939, 0, 1);
    Date begin = cal.getTime();
    cal.set(1939, 2, 1);
    Date end = cal.getTime();  
    TimePeriod period = new TimePeriod(begin, end);
    
    System.out.println("Files :");
    for(String name:period.getFilesNames()) {
      System.out.println(name);
    }
    
    List<String> folder = new LinkedList<String>();
    folder.add("/Users/christian/dhlabProject/JDG");
    
    Configuration config = new Configuration();
    
    RawArticleInputStream ras = new RawArticleInputStream(period, folder, config);
    
    RawArticle art;
    int limit = 0;
    while ((art = ras.read()) != null && limit < 100) {
        System.out.println(art.toString());
        limit ++;
    }
  }
}
