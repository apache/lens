package org.apache.lens.server.api.query;

import java.util.UUID;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.api.events.LensEvent;

import lombok.Getter;

/**
 * A generic event related to state change of a query Subclasses must declare the specific type of change they are
 * interested in.
 *
 * Every event will have an ID, which should be used by listeners to check if the event is already received.
 *
 * @param <T>
 *          Type of changed information about the query
 */
public abstract class QueryEvent<T> extends LensEvent {

  /** The previous value. */
  @Getter
  protected final T previousValue;

  /** The current value. */
  @Getter
  protected final T currentValue;

  /** The query handle. */
  @Getter
  protected final QueryHandle queryHandle;

  /** The id. */
  protected final UUID id = UUID.randomUUID();

  /**
   * Instantiates a new query event.
   *
   * @param eventTime
   *          the event time
   * @param prev
   *          the prev
   * @param current
   *          the current
   * @param handle
   *          the handle
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder("QueryEvent: ").append(getClass().getSimpleName()).append(":{id: ")
        .append(id).append(", query:").append(getQueryHandle()).append(", change:[").append(previousValue)
        .append(" -> ").append(currentValue).append("]}");
    return buf.toString();
  }
}
