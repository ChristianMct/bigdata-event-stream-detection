package org.epfl.bigdataevs.hmm;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class mainTestHmm {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    List<String> output = new LinkedList<String>();
    output.add("alpha");
    output.add("beta ");
    output.add("gamma");
    output.add("delta");
    output.add("epsil");
    double[] pi = {0.4,0.2,0.2,0.2};
    double[][] a = { { 0.4, 0.2, 0.2, 0.2 }, { 0.8, 0.2, 0, 0 }, { 0.7, 0, 0.3, 0 },
            { 0.2, 0, 0, 0.8 } };

    double[][] b = { { 0.2, 0.2, 0.2, 0.2, 0.2 }, { 0.8, 0.1, 0.1, 0, 0 },
            { 0.1, 0.8, 0.1, 0.0, 0 }, { 0, 0, 0, 0.2, 0.8 } };

    Hmm hmm = new Hmm(output, pi,a, b);

    List<String> observationSequence = hmm.generateObservationSequence(20000000);
    int[] hiddenSequence = hmm.decode(observationSequence);
    System.out.println("done decoding");
    /*
    String concat = "";
    for(String os : observationSequence)
      concat += os+" ";
    String states = "";
    for(int i:hiddenSequence)
      states += i+"     ";
    System.out.println(concat);
    System.out.println(states);
    */
      

  }
  
  

}
