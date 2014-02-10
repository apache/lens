package com.inmobi.grill.query;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@XmlRootElement
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryHandleWithResultSet extends QuerySubmitResult {

  @XmlElement @Getter private QueryHandle queryHandle;
  @XmlElement @Getter @Setter private QueryResult result;

  public QueryHandleWithResultSet(QueryHandle handle) {
    this.queryHandle = handle;
  }
}
