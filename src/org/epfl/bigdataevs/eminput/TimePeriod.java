package org.epfl.bigdataevs.eminput;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;


/**Represent an interval in time.
 * Team: Matias and Christian.**/

public class TimePeriod implements Serializable {
  
  public final Date from;
  public final Date to;
  
  public TimePeriod(Date from, Date to) {
    this.from = from;
    this.to = to;
  }
  
  public boolean equals(Object other) {
    if (other instanceof TimePeriod) {
      TimePeriod comparedTo = (TimePeriod)other;
      return (comparedTo.from.equals(from) && comparedTo.to.equals(to));
    }
    return false;
  }
  
  public int hashCode() {
    return from.hashCode() + to.hashCode();
  }
  
  public boolean lessThan(TimePeriod other) {
    return (this.to.compareTo(other.from) <= 0); 
  }
  
  /**Check if the given date is included in the TimePeriod.
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
  
  /**Test if a TimePeriod contains another TimePeriod
   * @param other the other TimePeriod
   * @return true if other is contained in this TimePeriod
   */
  public boolean contains(TimePeriod other) {
    return (this.from.getTime() <= other.from.getTime() && 
           this.to.getTime() >= other.to.getTime());
  }
  
  /**Returns the englobing TimePeriod that includes all given TimePeriod(s) in the list
   * @param allPeriods the list of all periods.
   * @return the TimePeriod englobing all periods in allPeriods
   */
  public static TimePeriod getEnglobingTimePeriod(List<TimePeriod> allPeriods) {
    TimePeriod first = allPeriods.get(0);
    Date minFrom = first.from;
    Date maxTo = first.to;
    for (TimePeriod period : allPeriods) {
      minFrom = period.from.before(minFrom) ? period.from : minFrom;
      maxTo = period.to.after(maxTo) ? period.to : maxTo;
    }
    return new TimePeriod(minFrom, maxTo);
  }
}
