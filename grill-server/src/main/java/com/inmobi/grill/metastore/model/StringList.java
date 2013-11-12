package com.inmobi.grill.metastore.model;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public class StringList {
  private List<String> elements;
  public StringList() {

  }

  public StringList(List<String> data) {
    elements = data;
  }

  public List<String> getElements() {
    return elements;
  }

  public void setElements(List<String> data) {
    elements = data;
  }
}
