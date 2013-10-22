package com.inmobi.grill.api;

import java.util.UUID;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class QueryPrepareHandle extends QuerySubmitResult {
  private UUID prepareHandleId;

  public QueryPrepareHandle() {
    
  }

  public QueryPrepareHandle(UUID handleId) {
    this.prepareHandleId = handleId;
  }

  public UUID getHandleId() {
    return prepareHandleId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((prepareHandleId == null) ? 0 : prepareHandleId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof QueryPrepareHandle)) {
      return false;
    }
    QueryPrepareHandle other = (QueryPrepareHandle) obj;
    if (prepareHandleId == null) {
      if (other.prepareHandleId != null) {
        return false;
      }
    } else if (!prepareHandleId.equals(other.prepareHandleId)) {
      return false;
    }
    return true;
  }
  
  @Override
  public String toString() {
    return prepareHandleId.toString();
  }
}
