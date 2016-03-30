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

package org.apache.lens.client.model;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.client.LensStatement;

import lombok.extern.slf4j.Slf4j;

/**
 * This class can be used to create Proxy Lens Query objects. The Proxy objects support lazy initialization
 * of members of this class given a query handle and LensStatement.
 * <p>
 * In most cases the query handle information should suffice which is available locally, and only in a few cases
 * like {@link org.apache.lens.client.LensClient.LensClientResultSetWithStats}, extra information needs to be fetched
 * from Lens Server.
 * <p>
 * Note:This class if not meant to be instantiated by lens apps (using lens-client to interact with lens server)
 * directly
 */
@Slf4j
public class ProxyLensQuery extends LensQuery {

  private LensStatement statement;
  private QueryHandle queryHandle;
  private boolean isFullyInitialized;
  private LensQuery actualLensQuery;

  public ProxyLensQuery(LensStatement statement, QueryHandle queryHandle) {
    this.statement = statement;
    this.queryHandle = queryHandle;
  }

  @Override
  public QueryHandle getQueryHandle() {
    return this.queryHandle;
  }

  @Override
  public String getSubmittedUser() {
    return this.statement.getUser();
  }

  @Override
  public String getUserQuery() {
    return getQuery().getUserQuery();
  }

  @Override
  public Priority getPriority() {
    return getQuery().getPriority();
  }

  @Override
  public boolean isPersistent() {
    return getQuery().isPersistent();
  }

  @Override
  public String getSelectedDriverName() {
    return getQuery().getSelectedDriverName();
  }

  @Override
  public String getDriverQuery() {
    return getQuery().getDriverQuery();
  }

  @Override
  public QueryStatus getStatus() {
    return getQuery().getStatus();
  }

  @Override
  public String getResultSetPath() {
    return getQuery().getResultSetPath();
  }

  @Override
  public String getDriverOpHandle() {
    return getQuery().getDriverOpHandle();
  }

  @Override
  public LensConf getQueryConf() {
    return getQuery().getQueryConf();
  }

  @Override
  public long getSubmissionTime() {
    return getQuery().getSubmissionTime();
  }

  @Override
  public long getLaunchTime() {
    return getQuery().getLaunchTime();
  }

  @Override
  public long getDriverStartTime() {
    return getQuery().getDriverStartTime();
  }

  @Override
  public long getDriverFinishTime() {
    return getQuery().getDriverFinishTime();
  }

  @Override
  public long getFinishTime() {
    return getQuery().getFinishTime();
  }

  @Override
  public long getClosedTime() {
    return getQuery().getClosedTime();
  }

  @Override
  public String getQueryName() {
    return getQuery().getQueryName();
  }

  @Override
  public Integer getErrorCode() {
    return getQuery().getErrorCode();
  }

  @Override
  public String getErrorMessage() {
    return getQuery().getErrorMessage();
  }

  @Override
  public String getQueryHandleString() {
    return getQuery().getQueryHandleString();
  }

  private synchronized LensQuery getQuery() {
    if (!isFullyInitialized) {
      this.actualLensQuery = statement.getQuery(queryHandle);
      this.isFullyInitialized = true;
    }
    return actualLensQuery;
  }
}
