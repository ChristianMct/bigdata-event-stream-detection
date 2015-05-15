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
  public static int numberOfCountsBackgroundModelThreshold = 30;
  // Threshold for the minimum number of words in an article so that it is considered
  public static final int numberOfWordsInArticlesThreshold = 50;
  
  /**
   * Parameters regarding the EM algorithm
   */
  public static final int numberOfRunsEmAlgorithm = 1;
  public static final int numberOfIterationsEmAlgorithm = 25;
  public static int numberOfThemes = 10;
  public static double lambdaBackgroundModel = 0.95;
  public static double themeFilteringThreshold = 0.9;
  
  /**
   * Parameters regarding the Evolution Graph
   */
  public static double threshold = 8.;
  public static double logMax = 1000.;  
  public static String startDate = "20/10/1992-0";
  public static int dateStepSize = 5;
  public static int dateStepsNumber = 8;
  public static String outputFilename = "graph.dot";
  public static boolean totalVariationDistance = false;
  
  public static double maxPenWidth = 3.;
  public static String startDateHMM;
  public static int dateStepSizeHMM = 20;
  
  //HMM parameters
  public static final int BWBlockSize = 1024 * 64;
  public static final int ViterbiBlockSize = 1024 * 128;
  public static boolean forceSequentialBW = true;
  
  
  /**
   * useful function to set parameters
   */
  private static double getValue(Properties prop, String key, double defaultVal) {
    String val = prop.getProperty(key);
    return (val == null) ? defaultVal : Double.parseDouble(val);
  }
  
  
  private static int getValue(Properties prop, String key, int defaultVal) {
    String val = prop.getProperty(key);
    return (val == null) ? defaultVal : Integer.parseInt(val);
  }
  
  
  private static String getValue(Properties prop, String key, String defaultVal) {
    String val = prop.getProperty(key);
    return (val == null) ? defaultVal : val;
    
  }
  
  private static boolean getValue(Properties prop, String key, boolean defaultVal) {
    String val = prop.getProperty(key);
    return (val == null) ? defaultVal : Boolean.parseBoolean(val);
    
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
      threshold = getValue(prop, "threshold", threshold);
      logMax = getValue(prop, "logMax", logMax);  
      startDate = getValue(prop, "startDate", startDate);
      dateStepSize = getValue(prop, "dateStepSize", dateStepSize);
      dateStepsNumber = getValue(prop, "dateStepsNumber", dateStepsNumber);
      outputFilename = getValue(prop, "outputFilename", outputFilename);
      totalVariationDistance = getValue(prop, "totalVariationDistance", totalVariationDistance);
      
      numberOfCountsBackgroundModelThreshold = getValue(prop, "numberOfCountsBackgroundModelThreshold", numberOfCountsBackgroundModelThreshold);
      numberOfThemes = getValue(prop, "numberOfThemes", numberOfThemes);
      
      lambdaBackgroundModel = getValue(prop, "lambdaBackgroundModel", lambdaBackgroundModel); 
      themeFilteringThreshold = getValue(prop, "themeFilteringThreshold", themeFilteringThreshold); 
      
      startDateHMM = getValue(prop,"startDateHMM", startDateHMM);
      dateStepSizeHMM = getValue(prop, "dateStepSizeHMM",dateStepSizeHMM);
    } catch (IOException ex) {
      System.out.println("Parameter file not found -> using default values");
    } finally {
      if (input != null) {
          input.close();
          System.out.println("themeFilteringThreshold : "+themeFilteringThreshold);
      }
    }
  }
  
}


