/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.scheduler.util;

import org.apache.lens.server.api.LensConfConstants;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UtilityMethods {
  private UtilityMethods() {

  }

  /**
   *
   * @param conf
   * @return
   */
  public static BasicDataSource getDataSourceFromConf(Configuration conf) {
    BasicDataSource basicDataSource = new BasicDataSource();
    basicDataSource.setDriverClassName(
        conf.get(LensConfConstants.SERVER_DB_DRIVER_NAME, LensConfConstants.DEFAULT_SERVER_DB_DRIVER_NAME));
    basicDataSource
        .setUrl(conf.get(LensConfConstants.SERVER_DB_JDBC_URL, LensConfConstants.DEFAULT_SERVER_DB_JDBC_URL));
    basicDataSource
        .setUsername(conf.get(LensConfConstants.SERVER_DB_JDBC_USER, LensConfConstants.DEFAULT_SERVER_DB_USER));
    basicDataSource
        .setPassword(conf.get(LensConfConstants.SERVER_DB_JDBC_PASS, LensConfConstants.DEFAULT_SERVER_DB_PASS));
    basicDataSource.setDefaultAutoCommit(true);
    return basicDataSource;
  }
}
