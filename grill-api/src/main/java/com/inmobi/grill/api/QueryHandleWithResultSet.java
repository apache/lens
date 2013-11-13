package com.inmobi.grill.api;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.inmobi.grill.client.api.APIResult;

@XmlRootElement
public class QueryHandleWithResultSet extends QuerySubmitResult {

  @XmlElement
  private QueryHandle handle;

//  private GrillResultSet resultSet;

  @XmlElement
  private String result;
  public QueryHandleWithResultSet() {
  }

  public QueryHandleWithResultSet(QueryHandle handle) {
    this.handle = handle;
  }

  public QueryHandle getQueryHandle() {
    return handle;
  }

  //public GrillResultSet getResultSet() {
  //  return null;
  //}
  

  public void SetResult(String result) {
    this.result = result;
  }

  public String getResult() {
    return result;
  }

}
