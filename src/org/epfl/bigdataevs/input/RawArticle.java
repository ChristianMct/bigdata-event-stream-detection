package org.epfl.bigdataevs.input;

import java.io.Serializable;
import java.util.Date;

/**This class represents a raw article read from the XML stream.
 * It is immutable and provide a builder so it can be instanciated
 * incrementally at parse time.
 * @author Christian
 *
 */
public class RawArticle implements Serializable {

  public final int pageNumber;
  public final String fullText;
  public final String id;
  public final String name;
  public final Date issueDate;
  public final ArticleStream stream;


  /** All at once initializer.
   * @param pageNumber the page number of the article
   * @param fullText the full text article
   * @param id the id of the article
   * @param issueDate the string representation of the date
   * @param name the title of the article
   * @param stream the enum type of the stream article comes from
   */
  public RawArticle(int pageNumber, String fullText, String id,
          Date issueDate, String name, ArticleStream stream) {
    this.pageNumber = pageNumber;
    this.fullText = fullText;
    this.id = id;
    this.issueDate = issueDate;
    this.name = name;
    this.stream = stream;
  }
  
  
  @Override
  public String toString() {
    return "RawArticle for article id:" + this.id + " Date: " + this.issueDate.toString();
  }
}

/**The Builder of RawArticle used to provide an incremental way of building RawArticle instances.
 * @author Christian
 *
 */
class RawArticleBuilder implements Serializable{

  public int pageNumber;
  public String fullText;
  public String id;
  public String name;
  public Date issueDate;
  public ArticleStream stream;

  public RawArticleBuilder() {
    reset();
  }

  /** Builds an instance of RawArticle and resets this builder
   * @return an instance of RawArticle if all fields were set, null otherwise.
   */
  public RawArticle build() {
    if (pageNumber != -1
            && fullText != null
            && id != null
            && issueDate != null
            && name != null
            && stream != null) {
      RawArticle art = new RawArticle(pageNumber, fullText, id, issueDate, name,stream);
      this.reset();
      return art;
    } else {
      return null;
    }
  }

  /**Resets the builder instance.
   */
  public void reset() {
    pageNumber = -1;
    fullText = null;
    id = null;
    issueDate = null;
    name = null;
    stream = null;
  }
}