package org.epfl.bigdataevs.eminput;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class RawArticleInputStream {

  private String current;
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy-HH");
  private final RawArticleBuilder builder;
  private final TimePeriod timePeriod;
  private final List<Path> sourcePaths;
  private final Iterator<Path> sourcePathsIt;
  private final Configuration config;
  private final XMLInputFactory factory;
  
  private FileSystem fileSystem;
  private XMLStreamReader reader;

  private boolean initCompleted;
  private boolean articleCompleted;
  private boolean shouldSkipArticle;


  /**An input stream that reads all the file from a given period from the given
   * input folders.
   * @param timePeriod the TimePeriod object corresponding to the period
   * @param articleFolders The absolute paths of all the folders containing article
   *        files. Can be hdfs:// or local path. Without trailing /.
   * @param config A configuration object from Hadoop
   */
  public RawArticleInputStream(TimePeriod timePeriod, 
          List<String> articleFolders, Configuration config) {
    this.timePeriod = timePeriod;
    this.builder = new RawArticleBuilder();
    this.sourcePaths = new LinkedList<Path>();
    for (String folder: articleFolders) {
      for (String fileName: timePeriod.getFilesNames()) {
        sourcePaths.add(new Path(folder + '/' + fileName));
      }
    }
    this.sourcePathsIt = sourcePaths.iterator();
    this.config = config;
    this.factory = XMLInputFactory.newInstance();
  }

  /**Reads the next RawArticle from the current file. If the file is done, loads the new one.
   * Order in which the RawArticle arrive is not garanted.
   * @return a new RawArticle, or null if there is no more files to parse.
   * @throws XMLStreamException if xml parsing error.
   * @throws NumberFormatException if a xml node should contain an int but contains something else
   * @throws ParseException sometimes
   * @throws IOException sometime
   */
  public RawArticle read() throws XMLStreamException, NumberFormatException, 
    ParseException, IOException {
    if (!initCompleted) {
      init();
      initCompleted = true;
    }
    
    articleCompleted = false;
    shouldSkipArticle = false;
    while (reader != null && reader.hasNext()) {
      int event = reader.next();

      switch (event) {
        case XMLStreamConstants.START_ELEMENT:
          current = "";
          startElement(reader.getLocalName());
          break;
        case XMLStreamConstants.CHARACTERS:
          current += reader.getText();
          break;
        case XMLStreamConstants.END_ELEMENT:
          endElement(reader.getLocalName());
          break;
        case XMLStreamConstants.END_DOCUMENT:
          closeStreamAndOpenNext();
          break;
        default:
          break;
      }   
      if (articleCompleted && !shouldSkipArticle) {
        return builder.build();
      }
    }
    return null;
  }

  private void init() throws IOException, XMLStreamException {
    this.fileSystem = FileSystem.get(config);
    if (this.sourcePathsIt.hasNext()) {      
      InputStream fileStream = this.fileSystem.open(this.sourcePathsIt.next());
      reader = factory.createXMLStreamReader(fileStream);
    }
  }
  
  private void closeStreamAndOpenNext() throws XMLStreamException, IOException {
    reader.close();
    reader = null;
    if (sourcePathsIt.hasNext()) {
      InputStream fileStream = this.fileSystem.open(sourcePathsIt.next());
      reader = this.factory.createXMLStreamReader(fileStream);
    }
  }

  private void startElement(String type) {
    if ("entity".equals(type)) {
      shouldSkipArticle = false;
      articleCompleted = false;
    }

    return;
  }

  private void endElement(String type) throws ParseException, NumberFormatException {
//    //if ("article".equals(type))
//    System.out.println("</"+type+">");
    switch (type) {
      case "entity":
        articleCompleted = true;
        break;
      case "name":
        builder.name = current;
        break;
      case "id":
        builder.id = current;
        break;
      case "page_no":
        builder.pageNumber = Integer.parseInt(current);
        break;
      case "publication":
        builder.stream = ArticleStream.valueOf(current);
        break;
      case "issue_date":
        //Append 12 so that the article date corresponds to noon of the issue date
        Date date = dateFormat.parse(current + "-12");
        builder.issueDate = date;
      //If the article falls outside TimePeriod, skip it
        shouldSkipArticle = !timePeriod.includeDates(date);
        break;
      case "word_count":
        builder.wordCount = Integer.parseInt(current);
        break;
      case "total_char_count":
        builder.charCount = Integer.parseInt(current);
        break;
      case "full_text":
        builder.fullText = current;
        break;
      default:
        break;
    }
  }


}
