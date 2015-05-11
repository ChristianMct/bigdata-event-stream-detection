package org.epfl.bigdataevs.evolutiongraph;

import org.epfl.bigdataevs.em.LightTheme;
import org.epfl.bigdataevs.em.Theme;

import java.io.Serializable;

public class EvolutionaryTransition implements Serializable {
  public LightTheme theme1;
  public LightTheme theme2;
  public double divergence;
  
  /**
   * @author antoinexp & lfaucon
   * 
   * @param t1 The first theme (chronological order)
   * @param t2 The second theme (chronological order)
   * @param divergence The Kullback divergence D(t1||t2). It shows the strength of the link
   *     between theme1 and theme2
   */
  public EvolutionaryTransition(LightTheme t1, LightTheme t2, double divergence) {
    this.theme1 = t1;
    this.theme2 = t2;
    this.divergence = divergence;
  }
  
  
  public String toString() {
    return "Evolutionary Transition\n"
            + "Theme 1 :\n\t" 
            + this.theme1.sortString(6,5) 
            + "\nTheme2 :\n\t" 
            + this.theme2.sortString(6,5) 
            + "\nDivergence :" 
            + this.divergence
            + "\n\n";
  }
}
