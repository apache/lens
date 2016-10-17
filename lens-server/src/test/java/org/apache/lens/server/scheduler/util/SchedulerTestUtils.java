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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import java.util.GregorianCalendar;
import java.util.UUID;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.query.events.QueryEnded;
import org.apache.lens.server.scheduler.SchedulerServiceImpl;

import org.apache.hadoop.conf.Configuration;

import org.powermock.api.mockito.PowerMockito;

public class SchedulerTestUtils {

  private SchedulerTestUtils() {

  }

  private static XTrigger getTestTrigger(String cron) {
    XTrigger trigger = new XTrigger();
    XFrequency frequency = new XFrequency();
    frequency.setCronExpression(cron);
    frequency.setTimezone("UTC");
    trigger.setFrequency(frequency);
    return trigger;
  }

  private static XExecution getTestExecution(String queryString) {
    XExecution execution = new XExecution();
    XJobQuery query = new XJobQuery();
    query.setQuery(queryString);
    execution.setQuery(query);
    XSessionType sessionType = new XSessionType();
    sessionType.setDb("default");
    execution.setSession(sessionType);
    return execution;
  }

  public static XJob getTestJob(String cron, String query, long start, long end) throws Exception {
    XJob job = new XJob();
    job.setTrigger(getTestTrigger(cron));
    job.setName("Test lens Job");
    GregorianCalendar startTime = new GregorianCalendar();
    startTime.setTimeInMillis(start);
    XMLGregorianCalendar startCal = DatatypeFactory.newInstance().newXMLGregorianCalendar(startTime);

    GregorianCalendar endTime = new GregorianCalendar();
    endTime.setTimeInMillis(end);
    XMLGregorianCalendar endCal = DatatypeFactory.newInstance().newXMLGregorianCalendar(endTime);

    job.setStartTime(startCal);
    job.setEndTime(endCal);
    job.setExecution(getTestExecution(query));
    return job;
  }

  public static void setupQueryService(SchedulerServiceImpl scheduler) throws Exception {
    QueryExecutionService queryExecutionService = PowerMockito.mock(QueryExecutionService.class);
    scheduler.setQueryService(queryExecutionService);
    PowerMockito
      .when(queryExecutionService.estimate(anyString(), any(LensSessionHandle.class), anyString(), any(LensConf.class)))
      .thenReturn(null);
    PowerMockito.when(
      queryExecutionService.executeAsync(any(LensSessionHandle.class), anyString(), any(LensConf.class), anyString()))
      .thenReturn(new QueryHandle(UUID.randomUUID()));
    PowerMockito.when(queryExecutionService.cancelQuery(any(LensSessionHandle.class), any(QueryHandle.class)))
      .thenReturn(true);
    scheduler.getSchedulerEventListener().setQueryService(queryExecutionService);
  }

  public static QueryEnded mockQueryEnded(SchedulerJobInstanceHandle instanceHandle, QueryStatus.Status status) {
    QueryContext mockContext = PowerMockito.mock(QueryContext.class);
    PowerMockito.when(mockContext.getResultSetPath()).thenReturn("/tmp/query1/result");
    Configuration conf = new Configuration();
    // set the instance handle
    conf.set("job_instance_key", instanceHandle.getHandleIdString());
    PowerMockito.when(mockContext.getConf()).thenReturn(conf);
    // Get the queryHandle.
    PowerMockito.when(mockContext.getQueryHandle()).thenReturn(new QueryHandle(UUID.randomUUID()));
    QueryEnded queryEnded = PowerMockito.mock(QueryEnded.class);
    PowerMockito.when(queryEnded.getQueryContext()).thenReturn(mockContext);
    PowerMockito.when(queryEnded.getCurrentValue()).thenReturn(status);
    return queryEnded;
  }
}
