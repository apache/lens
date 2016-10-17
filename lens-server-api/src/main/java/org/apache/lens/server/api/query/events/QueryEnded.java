/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.query.events;

import java.util.EnumSet;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.commons.lang.StringUtils;

import lombok.Getter;

/**
 * Generic event denoting that query has ended. If a listener wants to just be notified when query has ended
 * irrespective of its success or failure, then that listener can subscribe for this event type
 */
public class QueryEnded extends StatusChange {

  @Getter
  private final QueryContext queryContext;
  /**
   * The user.
   */
  @Getter
  private final String user;

  /**
   * The cause.
   */
  @Getter
  private final String cause;

  /**
   * The Constant END_STATES.
   */
  public static final EnumSet<QueryStatus.Status> END_STATES = EnumSet.of(QueryStatus.Status.SUCCESSFUL,
    QueryStatus.Status.CANCELED, QueryStatus.Status.CLOSED, QueryStatus.Status.FAILED);

  /**
   * Instantiates a new query ended.
   *
   * @param ctx
   * @param eventTime the event time
   * @param prev      the prev
   * @param current   the current
   * @param handle    the handle
   * @param user      the user
   * @param cause     the cause
   */
  public QueryEnded(QueryContext ctx, long eventTime, QueryStatus.Status prev, QueryStatus.Status current,
    QueryHandle handle, String user, String cause) {
    super(eventTime, prev, current, handle);
    this.queryContext = ctx;
    this.user = user;
    this.cause = cause;
    if (!END_STATES.contains(current)) {
      throw new IllegalStateException("Not a valid end state: " + current + " query: " + handle);
    }
  }

  public String toString() {
    StringBuilder buf = new StringBuilder(super.toString());
    if (StringUtils.isNotBlank(cause)) {
      buf.append(" cause:").append(cause);
    }
    return buf.toString();
  }


}
