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
package org.apache.lens.server.model;

import org.slf4j.MDC;

public class MappedDiagnosticLogSegregationContext implements LogSegregationContext {

  public static final String LOG_SEGREGATION_ID = "logSegregationId";
  public static final String QUERY_LOG_ID = "queryLogId";

  @Override
  public void setLogSegregationId(String id) {
    MDC.put(LOG_SEGREGATION_ID, id);
  }

  @Override
  public String getLogSegragationId() {
    return MDC.get(LOG_SEGREGATION_ID);
  }

  @Override
  public void setLogSegragationAndQueryId(String id) {
    setLogSegregationId(id);
    setQueryId(id);
  }

  public static void removeLogSegragationIds() {
    MDC.remove(LOG_SEGREGATION_ID);
    MDC.remove(QUERY_LOG_ID);
  }

  @Override
  public String getQueryId() {
    return MDC.get(QUERY_LOG_ID);
  }

  @Override
  public void setQueryId(String id) {
    MDC.put(QUERY_LOG_ID, id);
  }

  public static void put(String key, String value) {
    MDC.put(key, value);
  }
  public static void remove(String key) {
    MDC.remove(key);
  }
}
