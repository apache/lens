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
import org.apache.lens.server.api.events.LensEvent;

import java.util.UUID;

/**
 * Event fired when a query fails to purge. Use getCause() to get the reason for failure.
 */
public class QueryPurgeFailed extends QueryEvent<String> implements Alertable {
  protected final UUID id = UUID.randomUUID();
  /**
   * query context of the failed query
   */
  @Getter
  private final QueryContext context;
  /**
   * cause of purge failure. Can be used in handler
   */
  @Getter
  private final Exception cause;
  /**
   * Number of failed tries.
   */
  @Getter
  private final int numTries;
  private final HiveConf serverConf;

  public QueryPurgeFailed(QueryContext context, HiveConf serverConf, int numTries, Exception e) {
    super(System.currentTimeMillis(), context.getUserQuery(), context.getUserQuery(), context);
    this.context = context;
    this.serverConf = serverConf;
    this.cause = e;
    this.numTries = numTries;
  }

  @Override
  public String getLogMessage() {
    return "Query purge failed for query handle" + getQueryHandle() + ", retry no: " + numTries;
  }

  @Override
  public Email getEmail() {

    return new Email(
      serverConf.get(LensConfConstants.MAINTAINER_EMAIL_TO),
      serverConf.get(LensConfConstants.MAINTAINER_EMAIL_CC),
      serverConf.get(LensConfConstants.MAINTAINER_EMAIL_BCC),
      getEmailSubject(),
      getEmailMessage()
    );
  }


  public String getEmailSubject() {
    return "Query Purge Failed(" + context.getSubmittedUser() + "): " + getQueryHandle();
  }

  public String getEmailMessage() {
    return String.format(
      "User query: %s\n\n"
      + "Submission Time: %s\n"
    , context.getUserQuery(), context.getSubmissionTime());
  }
}
