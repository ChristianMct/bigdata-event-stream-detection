package org.epfl.bigdataevs.executables;


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
  // THreshold for the minimum numebr of words in an article so that it is considered
  public static final int numberOfWordsInArticlesThreshold = 50;
  
  /**
   * Parameters regarding the EM algorithm
   */
  public static final int numberOfRunsEmAlgorithm = 1;
  public static final int numberOfIterationsEmAlgorithm = 25;
  public static final int numberOfThemes = 10;
  public static final double lambdaBackgroundModel = 0.95;
  public static final double themeFilteringThreshold = 0.9;
}
