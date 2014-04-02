package com.inmobi.grill.server.api.query;

import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.server.api.events.GrillEvent;

import java.util.UUID;

import lombok.Getter;

/**
 * A generic event related to state change of a query
 * Subclasses must declare the specific type of change they are interested in.
 *
 * Every event will have an ID, which should be used by listeners to check if the event is already received.
 *
 * @param <T> Type of changed information about the query
 */
public abstract class QueryEvent<T> extends GrillEvent {
  @Getter protected final T previousValue;
  @Getter protected final T currentValue;
  @Getter protected final QueryHandle queryHandle;
  protected final UUID id = UUID.randomUUID();

  public QueryEvent(long eventTime, T prev, T current, QueryHandle handle) {
    super(eventTime);
    previousValue = prev;
    currentValue = current;
    this.queryHandle = handle;
  }

  @Override
  public String getEventId() {
    return id.toString();
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder("QueryEvent: ").append(getClass().getSimpleName())
    .append(":{id: ").append(id).append(", query:")
      .append(getQueryHandle())
      .append(", change:[").append(previousValue).append(" -> ").append(currentValue).append("]}");
    return buf.toString();
  }
}
