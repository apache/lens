package com.inmobi.grill.client.api;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "querylist")
public class QueryList extends APIResult {

  @XmlElement(name = "listname")
  private String listName;
  public QueryList(Status status, String msg) {
    super(status, msg);
    this.listName = "myname";
  }

  public QueryList() {
    super();
  }

  /**
   * @return the listName
   */
  public String getListName() {
    return listName;
  }

}
