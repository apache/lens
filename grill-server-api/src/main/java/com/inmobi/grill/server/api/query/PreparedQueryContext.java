package com.inmobi.grill.server.api.query;

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

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.query.GrillPreparedQuery;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.server.api.driver.GrillDriver;

import lombok.Getter;
import lombok.Setter;

public class PreparedQueryContext implements Delayed {
  @Getter private final QueryPrepareHandle prepareHandle;
  @Getter private final String userQuery;
  @Getter private final Date preparedTime;
  @Getter private final String preparedUser;
  transient @Getter private final Configuration conf;
  @Getter final GrillConf qconf;
  @Getter @Setter private GrillDriver selectedDriver;
  @Getter @Setter private String driverQuery;

  private static long millisInWeek = 7 * 24 * 60 * 60 * 1000;

  public PreparedQueryContext(String query, String user, Configuration conf) {
    this(query, user, conf, new GrillConf());
  }

  public PreparedQueryContext(String query, String user, Configuration conf, GrillConf qconf) {
    this.userQuery = query;
    this.preparedTime = new Date();
    this.preparedUser = user;
    this.prepareHandle = new QueryPrepareHandle(UUID.randomUUID());
    this.conf = conf;
    this.qconf = qconf;
    this.driverQuery = query;
  }

  @Override
  public int compareTo(Delayed o) {
    return (int)(this.getDelay(TimeUnit.MILLISECONDS)
        - o.getDelay(TimeUnit.MILLISECONDS));
  }

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
   * @param confoverlay the conf to set
   */
  public void updateConf(Map<String,String> confoverlay) {
    qconf.getProperties().putAll(confoverlay);
    for (Map.Entry<String,String> prop : confoverlay.entrySet()) {
      this.conf.set(prop.getKey(), prop.getValue());
    }
  }

  public GrillPreparedQuery toPreparedQuery() {
    return new GrillPreparedQuery(prepareHandle, userQuery, preparedTime,
        preparedUser,
        selectedDriver != null ? selectedDriver.getClass().getCanonicalName() : null,
        driverQuery, qconf);
  }

}
