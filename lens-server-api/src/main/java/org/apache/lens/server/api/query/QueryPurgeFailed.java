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
package org.apache.lens.server.api.query;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.events.LensEvent;

import java.util.UUID;

/**
 * Event fired when a query fails to purge. Use getCause() to get the reason for failure.
 */
public class QueryPurgeFailed extends LensEvent {
  protected final UUID id = UUID.randomUUID();
  private final QueryContext context;
  private final Exception cause;

  public QueryPurgeFailed(QueryContext context, Exception e) {
    super(System.currentTimeMillis());
    this.context = context;
    this.cause = e;
  }

  @Override
  public String getEventId() {
    return id.toString();
  }

  public QueryContext getContext() {
    return context;
  }

  public Exception getCause() {
    return cause;
  }
}
