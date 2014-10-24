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
package org.apache.lens.server.stats.event;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Loggable Lens Statistics which is logged to a log4j file as a JSON Object.
 */
public abstract class LoggableLensStatistics extends LensStatistics {

  /**
   * Instantiates a new loggable lens statistics.
   *
   * @param eventTime
   *          the event time
   */
  public LoggableLensStatistics(long eventTime) {
    super(eventTime);
  }

  /**
   * Instantiates a new loggable lens statistics.
   */
  public LoggableLensStatistics() {
    this(System.currentTimeMillis());
  }

  /**
   * Gets the hive table.
   *
   * @param conf
   *          the conf
   * @return the hive table
   */
  public abstract Table getHiveTable(Configuration conf);
}
