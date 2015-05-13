package org.epfl.bigdataevs.executables;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * Parameters that are used in the project are grouped in this class as final static objects.
 * 
 * @author abastien
 */
public class Parameters {

  /**
   * Parameters regarding time periods
   */
  public static final String backgroundModelStartDate = "";
  public static final String backgroundModelEndDate = "";
  public static final String timePeriodsStartDate = "";
  public static final int timePeriodsLength = 7;
  public static final int numberOfTimePeriods = 10;
  
  /**
   * Parameters regarding the cleaning of the articles
   */
  
  // Threshold for the first number of pages of the newspaper edition to be considered
  public static final int firstNumberOfPagesInNewspaperThreshold = 3;
  // Threshold for the minimum number of count for a word to be considered in the background model
  public static final int numberOfCountsBackgroundModelThreshold = 50;
  // Threshold for the minimum number of words in an article so that it is considered
  public static final int numberOfWordsInArticlesThreshold = 50;
  
  /**
   * Parameters regarding the EM algorithm
   */
  public static final int numberOfRunsEmAlgorithm = 1;
  public static final int numberOfIterationsEmAlgorithm = 25;
  public static final int numberOfThemes = 10;
  public static final double lambdaBackgroundModel = 0.95;
  public static final double themeFilteringThreshold = 0.9;
  
  /**
   * Parameters regarding the Evolution Graph
   */
  public static double threshold = 8.;
  public static double logMax = 1000.;  
  public static String startDate = "20/10/1992-0";
  public static int dateStepSize = 5;
  public static int dateStepsNumber = 8;
  public static String outputFilename = "graph.dot";
  
  public static double maxPenWidth = 3.;
  
  
  /**
   * useful function to set parameters
   */
  private static double getDouble(Properties prop, String key, double defaultVal){
    String val = prop.getProperty(key);
    return (val == null) ? defaultVal : Double.parseDouble(val);
  }
  
  
  private static int getInt(Properties prop, String key, int defaultVal){
    String val = prop.getProperty(key);
    return (val == null) ? defaultVal : Integer.parseInt(val);
  }
  
  
  private static String getString(Properties prop, String key, String defaultVal){
    String val = prop.getProperty(key);
    return (val == null) ? defaultVal : val;
    
  }
  
  /**
   * Load parameters in a configuration file
   * @param filename is the configuration file name
   * @throws IOException
   */
  public static void parseParameters(String filename) throws IOException{
    Properties prop = new Properties();
    InputStream input = null;
   
    try {
   
      input = new FileInputStream(filename);
   
      // load a properties file
      prop.load(input);
   
      // get the property value
      threshold = getDouble(prop, "threshold", threshold);
      logMax = getDouble(prop, "logMax", logMax);  
      startDate = getString(prop, "startDate", startDate);
      dateStepSize = getInt(prop, "dateStepSize", dateStepSize);
      dateStepsNumber = getInt(prop, "dateStepsNumber", dateStepsNumber);
      outputFilename = getString(prop, "outputFilename", outputFilename);
      
    } catch (IOException ex) {
      System.out.println("Parameter file not found -> using default values");
    } finally {
      if (input != null) {
          input.close();
      }
    }
  }
  
}


