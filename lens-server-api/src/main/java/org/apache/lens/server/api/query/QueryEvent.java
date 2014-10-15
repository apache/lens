package org.apache.lens.server.api.query;

/*
 * #%L
 * Grill API for server and extensions
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import java.util.UUID;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.api.events.LensEvent;

import lombok.Getter;

/**
 * A generic event related to state change of a query
 * Subclasses must declare the specific type of change they are interested in.
 *
 * Every event will have an ID, which should be used by listeners to check if the event is already received.
 *
 * @param <T> Type of changed information about the query
 */
public abstract class QueryEvent<T> extends LensEvent {
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
