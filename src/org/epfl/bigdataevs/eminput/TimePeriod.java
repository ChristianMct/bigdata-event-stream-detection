package org.epfl.bigdataevs.eminput;

import org.apache.hadoop.fs.Path;

import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;


/**Team: Matias and Christian.**/
public class TimePeriod {
  
  public final Date from;
  public final Date to;
  
  public TimePeriod(Date from, Date to) {
    this.from = from;
    this.to = to;
  }
  
  public boolean lessThan(TimePeriod p) {
    return (this.to.compareTo(p.from) <= 0); 
  }

  public List<String> getFilesNames() {
    List<String> names = new LinkedList<String>();

    for (int year = from.getYear(); year <= to.getYear();  year++) {
      names.add("articles"+(year+1900)+".xml");
    }
    
    return names;
  }
}
