package org.epfl.bigdataevs.hmm;

public class Utils {
  
  
  /**Computes the norm one of this vector or matrix.
   * 
   * @param t the array on which we want to compute norm 1
   * @return
   */
  public static double normOne(double[] t){
    int n = t.length;
    double sum = 0.0;
    for(int i = 0;i<n;i++){
      sum += t[i];
    }
    return sum;
  }
  

  
  
  
  
}
