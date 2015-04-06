package org.epfl.bigdataevs.eminput;

import java.io.Serializable;
import java.util.Map;

/**Team: Matias and Christian.
 * Container for the data of processed articles. An instance 
 * of this class contains the cleaned words and their count
 * in this article, as well as the stream identifier.**/
public class ParsedArticle implements Serializable {
  /**Maps a word to the number of times it appears in this article.**/
  public final Map<String, Integer> words;
  /**This article's stream identifier. **/
  public final ArticleStream stream;
  
  public ParsedArticle( Map<String, Integer> words, ArticleStream stream) {
    this.words = words;
    this.stream = stream;
  }
}