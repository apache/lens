package com.inmobi.grill.api;

import java.util.UUID;

public class QueryHandle {
  private final UUID handleId;

  public QueryHandle(UUID handleId) {
    this.handleId = handleId;
  }

  public UUID getHandleId() {
    return handleId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((handleId == null) ? 0 : handleId.hashCode());
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
    if (!(obj instanceof QueryHandle)) {
      return false;
    }
    QueryHandle other = (QueryHandle) obj;
    if (handleId == null) {
      if (other.handleId != null) {
        return false;
      }
    } else if (!handleId.equals(other.handleId)) {
      return false;
    }
    return true;
  }
}
