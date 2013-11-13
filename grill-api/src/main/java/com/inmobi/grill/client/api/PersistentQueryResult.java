package com.inmobi.grill.client.api;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PersistentQueryResult extends QueryResult {

  @XmlElement
  private String persistedURI;

  public PersistentQueryResult() {
  }

  public PersistentQueryResult(String persistedURI) {
    this.persistedURI = persistedURI;
  }

  public String getPersistedURI() {
    return persistedURI;
  }
}
