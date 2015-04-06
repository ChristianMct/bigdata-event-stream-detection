package org.epfl.bigdataevs.hmm;

import java.util.Date;


/**A class that encapsulates all the tools necessary to perform theme life cycle analysis 
 * (paper section 4).
 * 
 * @author team Damien-Laurent-Sami
 *
 */
public class LifeCycleAnalyser {
  
  private final Date beginningOfPeriod;
  private final Date endOfPeriod;
  
  
  /**Basic LifeCycleAnalyser constructor.
   * 
   * @param beginning the beginning date of the studied period
   * @param end the ending date of the studied period
   */
  public LifeCycleAnalyser(Date beginning, Date end) {
    beginningOfPeriod = beginning;
    endOfPeriod = end;
    
  }
  
  /**Method that builds the HMM and does all the learning.
   * This method needs to be called before any visualization tool.
   * 
   */
  public void analyze(){
    //construct the HMM by calling constructor
    
    //train the HMM
    
    //decode the sequence with the HMM
    
    //exploit the states sequence to measure the strength of each theme over time
    
    
  }
  
  //TODO visualization tools
  
  
  
  public Date getBeginningOfPeriod() {
    return beginningOfPeriod;
  }

  public Date getEndOfPeriod() {
    return endOfPeriod;
  }

}
