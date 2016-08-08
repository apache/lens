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

package org.apache.lens.server.query;


import java.util.Map;

import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryEnded;
import org.apache.lens.server.api.query.QueryEvent;
import org.apache.lens.server.model.LogSegregationContext;

import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryEndHttpNotifier extends QueryEventHttpNotifier<QueryEnded> {

  private final LogSegregationContext logSegregationContext;
  private static final int CORE_POOL_SIZE = 3;

  public QueryEndHttpNotifier(Configuration config, LogSegregationContext logSegregationContext) {
    super(config, CORE_POOL_SIZE);
    this.logSegregationContext = logSegregationContext;
  }

  @Override
  public void process(QueryEnded event) {
    logSegregationContext.setLogSegragationAndQueryId(event.getQueryContext().getQueryHandleString());
    super.process(event, event.getQueryContext());
  }

  @Override
  protected boolean isHttpNotificationEnabled(QueryEvent event, QueryContext queryContext) {
    return (event.getCurrentValue() != QueryStatus.Status.CLOSED) // CLOSED QueryEnded events to be skipped
      && super.isHttpNotificationEnabled(event, queryContext);
  }

  @Override
  protected void updateExtraEventDetails(QueryEvent event, QueryContext queryContext,
    Map<String, Object> eventDetails) {
    //Nothing specific as of now for finished queries. We can attach query results later if required.
  }

  @Override
  protected NotificationType getNotificationType() {
    return NotificationType.FINISHED;
  }
}
