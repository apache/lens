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

import static org.testng.Assert.*;

import org.slf4j.MDC;
import org.testng.annotations.Test;

/**
 * Tests for MDC log segregation
 */
public class TestMappedDiagnosticLogSegregationContext {

  @Test
  public void testIds() {
    MappedDiagnosticLogSegregationContext testLogCtx = new MappedDiagnosticLogSegregationContext();
    String logId = "logid";
    testLogCtx.setLogSegregationId(logId);
    assertEquals(testLogCtx.getLogSegragationId(), logId);
    assertEquals(MDC.get(MappedDiagnosticLogSegregationContext.LOG_SEGREGATION_ID), logId);

    String queryId = "queryId";
    testLogCtx.setLogSegragationAndQueryId(queryId);
    assertEquals(testLogCtx.getQueryId(), queryId);
    assertEquals(testLogCtx.getLogSegragationId(), queryId);
    assertEquals(MDC.get(MappedDiagnosticLogSegregationContext.LOG_SEGREGATION_ID), queryId);
    assertEquals(MDC.get(MappedDiagnosticLogSegregationContext.QUERY_LOG_ID), queryId);

    String logId2 = "logid2";
    testLogCtx.setLogSegregationId(logId2);
    assertEquals(testLogCtx.getLogSegragationId(), logId2);
    assertEquals(MDC.get(MappedDiagnosticLogSegregationContext.LOG_SEGREGATION_ID), logId2);
    assertEquals(testLogCtx.getQueryId(), queryId);
    assertEquals(MDC.get(MappedDiagnosticLogSegregationContext.QUERY_LOG_ID), queryId);

    String queryId2 = "queryId2";
    testLogCtx.setQueryId(queryId2);
    assertEquals(testLogCtx.getQueryId(), queryId2);
    assertEquals(testLogCtx.getLogSegragationId(), logId2);
    assertEquals(MDC.get(MappedDiagnosticLogSegregationContext.LOG_SEGREGATION_ID), logId2);
    assertEquals(MDC.get(MappedDiagnosticLogSegregationContext.QUERY_LOG_ID), queryId2);
  }
}
