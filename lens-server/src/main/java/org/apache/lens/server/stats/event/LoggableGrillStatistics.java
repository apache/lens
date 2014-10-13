package org.apache.lens.server.stats.event;
 /*
 * #%L
 * Grill Server
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Loggable Grill Statistics which is logged to a log4j file as a JSON Object.
 */
public abstract class LoggableGrillStatistics extends GrillStatistics {

  public LoggableGrillStatistics(long eventTime) {
    super(eventTime);
  }

  public LoggableGrillStatistics() {
    this(System.currentTimeMillis());
  }

  public abstract Table getHiveTable(Configuration conf);
}
