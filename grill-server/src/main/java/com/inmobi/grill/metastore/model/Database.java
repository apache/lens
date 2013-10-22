package com.inmobi.grill.metastore.model;


import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Database {
  private String name;
  public Database() {

  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
