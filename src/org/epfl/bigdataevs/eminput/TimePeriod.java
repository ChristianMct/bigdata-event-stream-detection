package org.epfl.bigdataevs.eminput;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;


/**Represent an interval in time.
 * Team: Matias and Christian.**/
public class TimePeriod {
  
  public final Date from;
  public final Date to;
  
  public TimePeriod(Date from, Date to) {
    this.from = from;
    this.to = to;
  }
  
  public boolean lessThan(TimePeriod other) {
    return (this.to.compareTo(other.from) <= 0); 
  }
  
  /**Check if the given date is included in the TimePeriod
   * @param date the date to check
   * @return true if the date is included, false otherwise
   */
  public boolean includeDates(Date date) {
    return from.before(date) && to.after(date);
  }

  /**Generates the list of file names concerned by the time period.
   * @return a List of filenames of the forme "articles1XXX.xml"
   */
  public List<String> getFilesNames() {
    List<String> names = new LinkedList<String>();

    for (int year = from.getYear(); year <= to.getYear();  year++) {
      int yearFull = year + 1900;
      names.add("articles" + yearFull + ".xml");
    }
    
    return names;
  }
}
