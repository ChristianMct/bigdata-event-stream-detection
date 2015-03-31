package org.epfl.bigdataevs.eminput;

import java.util.Date;

public class TimePeriod {
  
  public final Date from;
  public final Date to;
  
  public TimePeriod(Date from, Date to) {
    this.from = from;
    this.to = to;
  }
}
