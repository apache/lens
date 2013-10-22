package com.inmobi.grill.api;

import javax.xml.bind.annotation.XmlRootElement;

import com.inmobi.grill.client.api.APIResult;

@XmlRootElement
public class QueryHandleWithResultSet extends QuerySubmitResult {
  public QueryHandle getQueryHandle() {
    return null;
  }
  public GrillResultSet getResultSet() {
    return null;
  }
}
