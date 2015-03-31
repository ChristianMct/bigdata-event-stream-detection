package org.epfl.bigdataevs.eminput;

import java.io.Serializable;
import java.util.Map;

public class ParsedArticle implements Serializable {
  public final Map<String, Integer> words;
  public final ArticleStream stream;
  
  public ParsedArticle( Map<String, Integer> words, ArticleStream stream) {
    this.words = words;
    this.stream = stream;
  }
}
