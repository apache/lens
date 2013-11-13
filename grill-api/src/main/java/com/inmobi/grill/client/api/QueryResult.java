package com.inmobi.grill.client.api;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import com.inmobi.grill.api.GrillResultSet;

@XmlRootElement
@XmlSeeAlso({PersistentQueryResult.class})
public class QueryResult {

  public QueryResult() {
  }

  public QueryResult(GrillResultSet result) {
    
  }
}
