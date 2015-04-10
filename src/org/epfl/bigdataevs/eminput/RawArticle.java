package org.epfl.bigdataevs.eminput;

import java.io.Serializable;
import java.util.Date;

/**This class represents a raw article read from the XML stream.
 * It is immutable and provide a builder so it can be instanciated
 * incrementally at parse time.
 * @author Christian
 *
 */
public class RawArticle implements Serializable {

  public final int charCount;
  public final int pageNumber;
  public final int wordCount;
  public final String fullText;
  public final String id;
  public final String name;
  public final Date issueDate;
  public final ArticleStream stream;


  /** All at once initializer.
   * @param charCount the number of characters in article
   * @param pageNumber the page number of the article
   * @param wordCount the number of word in article
   * @param fullText the full text article
   * @param id the id of the article
   * @param issueDate the string representation of the date
   * @param name the title of the article
   * @param stream the enum type of the stream article comes from
   */
  public RawArticle(int charCount, int pageNumber, int wordCount, 
          String fullText, String id, Date issueDate, String name, 
          ArticleStream stream) {
    this.charCount = charCount;
    this.pageNumber = pageNumber;
    this.wordCount = pageNumber;
    this.fullText = fullText;
    this.id = id;
    this.issueDate = issueDate;
    this.name = name;
    this.stream = stream;
  }

  /**The Builder of RawArticle used to provide an incremental way of building RawArticle instances.
   * @author Christian
   *
   */
  class RawArticleBuilder {

    public int charCount;
    public int pageNumber;
    public int wordCount;
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
      if ( charCount != -1
              && pageNumber != -1
              && wordCount != -1
              && fullText != null
              && id != null
              && issueDate != null
              && name != null
              && stream != null) {
        RawArticle art = new RawArticle(charCount, pageNumber, 
                wordCount, fullText, id, issueDate, name,stream);
        this.reset();
        return art;
      } else {
        return null;
      }
    }

    /**Resets the builder instance.
     */
    public void reset() {
      charCount = -1;
      pageNumber = -1;
      wordCount = -1;
      fullText = null;
      id = null;
      issueDate = null;
      name = null;
      stream = null;
    }
  }
}
