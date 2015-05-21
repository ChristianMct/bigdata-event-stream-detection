package org.epfl.bigdataevs.input;

import org.apache.commons.lang3.tuple.Pair;
import org.epfl.bigdataevs.em.EmAlgo;
import org.epfl.bigdataevs.em.Theme;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
  /**Publication date**/
  public final Date publication;
  /**Article's title**/
  public final String title;
  
public ParsedArticle( Map<String, Integer> words, ArticleStream stream, Date publication, 
        String title) {
    this.words = words;
    this.stream = stream;
    this.publication = publication;
    this.title = title;
  }
}
