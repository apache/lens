package com.inmobi.grill.api;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ResultColumn {

  @XmlElement
  private String name;
  @XmlElement
  private String type;

  public ResultColumn() {
  }

  public ResultColumn(String name, String type) {
    this.name = name;
    this.type = type;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the type
   */
  public String getType() {
    return type;
  }

  @Override
  public String toString() {
    return new StringBuilder(name).append(':').append(type).toString();
  }
}