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

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.LensPreparedQuery;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.server.api.driver.LensDriver;

import lombok.Getter;
import lombok.Setter;

/**
 * The Class PreparedQueryContext.
 */
public class PreparedQueryContext implements Delayed {

  /** The prepare handle. */
  @Getter
  private final QueryPrepareHandle prepareHandle;

  /** The user query. */
  @Getter
  private final String userQuery;

  /** The prepared time. */
  @Getter
  private final Date preparedTime;

  /** The prepared user. */
  @Getter
  private final String preparedUser;

  /** The conf. */
  transient @Getter private final Configuration conf;

  /** The qconf. */
  @Getter
  final LensConf qconf;

  /** The selected driver. */
  @Getter
  @Setter
  private LensDriver selectedDriver;

  /** The driver query. */
  @Getter
  @Setter
  private String driverQuery;

  /** The query name. */
  @Getter
  @Setter
  private String queryName;

  /** The millis in week. */
  private static long millisInWeek = 7 * 24 * 60 * 60 * 1000;

  /**
   * Instantiates a new prepared query context.
   *
   * @param query
   *          the query
   * @param user
   *          the user
   * @param conf
   *          the conf
   */
  public PreparedQueryContext(String query, String user, Configuration conf) {
    this(query, user, conf, new LensConf());
  }

  /**
   * Instantiates a new prepared query context.
   *
   * @param query
   *          the query
   * @param user
   *          the user
   * @param conf
   *          the conf
   * @param qconf
   *          the qconf
   */
  public PreparedQueryContext(String query, String user, Configuration conf, LensConf qconf) {
    this.userQuery = query;
    this.preparedTime = new Date();
    this.preparedUser = user;
    this.prepareHandle = new QueryPrepareHandle(UUID.randomUUID());
    this.conf = conf;
    this.qconf = qconf;
    this.driverQuery = query;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Delayed o) {
    return (int) (this.getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.concurrent.Delayed#getDelay(java.util.concurrent.TimeUnit)
   */
  @Override
  public long getDelay(TimeUnit units) {
    long delayMillis;
    if (this.preparedTime != null) {
      Date now = new Date();
      long elapsedMills = now.getTime() - this.preparedTime.getTime();
      delayMillis = millisInWeek - elapsedMills;
      return units.convert(delayMillis, TimeUnit.MILLISECONDS);
    } else {
      return Integer.MAX_VALUE;
    }
  }

  /**
   * Update conf.
   *
   * @param confoverlay
   *          the conf to set
   */
  public void updateConf(Map<String, String> confoverlay) {
    qconf.getProperties().putAll(confoverlay);
    for (Map.Entry<String, String> prop : confoverlay.entrySet()) {
      this.conf.set(prop.getKey(), prop.getValue());
    }
  }

  /**
   * To prepared query.
   *
   * @return the lens prepared query
   */
  public LensPreparedQuery toPreparedQuery() {
    return new LensPreparedQuery(prepareHandle, userQuery, preparedTime, preparedUser,
        selectedDriver != null ? selectedDriver.getClass().getCanonicalName() : null, driverQuery, qconf);
  }

}
