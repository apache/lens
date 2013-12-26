package com.inmobi.grill.server.api.events;


import com.inmobi.grill.api.QueryHandle;

import java.util.UUID;

public abstract class QueryEvent<T> {
  protected final T previousValue;
  protected final T currentValue;
  protected final QueryHandle handle;
  protected final UUID id = UUID.randomUUID();

  public QueryEvent(T prev, T current, QueryHandle handle) {
    previousValue = prev;
    currentValue = current;
    this.handle = handle;
  }

  public final T getPreviousValue() {
    return previousValue;
  }

  public final T getCurrentValue() {
    return currentValue;
  }

  public final QueryHandle getQueryHandle() {
    return handle;
  }

  public final UUID getId() {
    return id;
  }
}
