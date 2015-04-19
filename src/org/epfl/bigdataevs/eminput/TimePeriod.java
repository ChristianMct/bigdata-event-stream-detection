package org.epfl.bigdataevs.eminput;

import java.io.Serializable;
import java.util.Date;

/**Team: Matias and Christian**/
public class TimePeriod implements Serializable {
  
  public final Date from;
  public final Date to;
  
  public TimePeriod(Date from, Date to) {
    this.from = from;
    this.to = to;
  }
  
  public boolean lessThan(TimePeriod p){
    return (this.to.compareTo(p.from) <= 0); 
  }
}
