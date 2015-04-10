package org.epfl.bigdataevs.eminput;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.epfl.bigdataevs.eminput.RawArticle.RawArticleBuilder;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.StartElement;

public class RawArticleInputStream {

  private String current;
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
  private final RawArticleBuilder builder;
  private final TimePeriod timePeriod;
  private XMLStreamReader reader;

 
  
  public RawArticleInputStream(TimePeriod timePeriod) {
    this.timePeriod = timePeriod;
    this.builder = new RawArticleBuilder();
    init();
  }
  
  public RawArticle read() throws XMLStreamException{
    
    while (reader.hasNext()) {
      int event = reader.next();
      
      switch(event) {
      case XMLStreamConstants.START_ELEMENT:
        startElement(reader.getLocalName());
        break;
      case XMLStreamConstants.CHARACTERS:
        current = reader.getText();
        break;
      case XMLStreamConstants.END_ELEMENT:
        endElement(reader.getLocalName());
        break;
      }
    }
  }
  
  private void init() throws IOException, XMLStreamException {
    Path path = new Path("hdfs:// ...");
    FileSystem fs = FileSystem.get(new Configuration());
    XMLInputFactory factory = XMLInputFactory.newInstance();
    reader = factory.createXMLStreamReader(fs.open(path));
  }
  
  private void startElement(String type) {
    switch(type) {
    
    }
  }
  
  private void endElement(String type) throws ParseException, NumberFormatException {
    switch(type){
    case "article":
      
      break;
    case "name":
      builder.name = current;
      break;
    case "id":
      builder.id = current;
      break;
    case "page_number":
      builder.pageNumber = Integer.parseInt(current);
      break;
    case "publication":
      builder.stream = ArticleStream.valueOf(current);
    case "issue_date":
      builder.issueDate = dateFormat.parse(current);
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
    }
  }
  
  
}
