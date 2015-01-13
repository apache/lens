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

import lombok.Getter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.alerts.Alertable;
import org.apache.lens.server.api.alerts.Email;

import java.util.EnumSet;

/**
 * Generic event denoting that query has ended. If a listener wants to just be notified when query has ended
 * irrespective of its success or failure, then that listener can subscribe for this event type
 */
public class QueryEnded extends StatusChange implements Alertable {

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
  @Getter
  protected final HiveConf serverConf;

  /**
   * Instantiates a new query ended.
   *
   * @param eventTime the event time
   * @param prev      the prev
   * @param current   the current
   * @param ctx       the context
   * @param user      the user
   * @param cause     the cause
   * @param hiveConf
   */
  public QueryEnded(long eventTime, QueryStatus.Status prev, QueryStatus.Status current, QueryContext ctx,
    String user, String cause, HiveConf hiveConf) {
    super(eventTime, prev, current, ctx);
    this.user = user;
    this.cause = cause;
    this.serverConf = hiveConf;
    if (!END_STATES.contains(current)) {
      throw new IllegalStateException("Not a valid end state: " + current + " query: " + getQueryHandle());
    }
  }

  @Override
  public String getLogMessage() {
    return "Sending query completion message for handle " + getQueryContext().getQueryHandle() + ", query name: "
      + getQueryContext().getQueryName() + ", status: " + currentValue + " to " + queryContext.getSubmittedUser();

  }

  @Override
  public Email getEmail() {
    String to = queryContext.getSubmittedUser() + "@" + serverConf.get(LensConfConstants.SERVER_DOMAIN);
    String cc = queryContext.getConf().get(LensConfConstants.QUERY_RESULT_EMAIL_CC,
      LensConfConstants.QUERY_RESULT_DEFAULT_EMAIL_CC);

    return new Email(to, cc, null, getEmailSubject(), getEmailMessage());
  }

  public String getEmailSubject() {
    return "Query " + getQueryContext().getQueryName() + " " + currentValue + ": "
      + getQueryHandle();
  }

  public String getEmailMessage() {
    return "";
  }
}
