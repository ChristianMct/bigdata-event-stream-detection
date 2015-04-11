package org.epfl.bigdataevs.eminput;


import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class RawArticleInputStream {

  private String current;
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
  private final RawArticleBuilder builder;
  private final TimePeriod timePeriod;
  private XMLStreamReader reader;
  private InputStream inputStream;

  private boolean initCompleted;
  private boolean articleCompleted;
  private boolean shouldSkipArticle;

  public RawArticleInputStream(TimePeriod timePeriod, InputStream inputStream) {
    this.timePeriod = timePeriod;
    this.builder = new RawArticleBuilder();
    this.inputStream = inputStream;
  }

  public RawArticle read() throws XMLStreamException, NumberFormatException, 
    ParseException, IOException {
    if (!initCompleted) {
      init();
      initCompleted = true;
    }
    
    articleCompleted = false;
    shouldSkipArticle = false;
    while (reader.hasNext()) {
      int event = reader.next();

      switch (event) {
        case XMLStreamConstants.START_ELEMENT:
          startElement(reader.getLocalName());
          break;
        case XMLStreamConstants.CHARACTERS:
          current = reader.getText();
          break;
        case XMLStreamConstants.END_ELEMENT:
          endElement(reader.getLocalName());
          break;
        default:
          break;
      }   
      if (articleCompleted) {
        return builder.build();
      }
    }
    return null;
  }

  private void init() throws IOException, XMLStreamException {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    reader = factory.createXMLStreamReader(inputStream);
  }

  private void startElement(String type) {
    return;
  }

  private void endElement(String type) throws ParseException, NumberFormatException {
    switch (type) {
      case "article":
        articleCompleted = true;
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
        break;
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
      default:
        break;
    }
  }


}
