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
package org.apache.lens.server.stats;

import java.io.ByteArrayOutputStream;

import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.stats.event.LoggableLensStatistics;
import org.apache.lens.server.stats.store.log.LogStatisticsStore;
import org.apache.lens.server.stats.store.log.StatisticsLogLayout;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Table;

import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The Class TestLogStatisticsStore.
 */
@Test(groups = "unit-test")
public class TestLogStatisticsStore {

  /**
   * The Class MyLoggableLens.
   */
  private static class MyLoggableLens extends LoggableLensStatistics {

    /**
     * Instantiates a new my loggable lens.
     *
     * @param eventTime the event time
     */
    public MyLoggableLens(long eventTime) {
      super(eventTime);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.stats.event.LoggableLensStatistics#getHiveTable(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public Table getHiveTable(HiveConf conf) {
      return null;
    }

    @Override
    public String getEventId() {
      return "random";
    }
  }

  /**
   * Test log statistics store.
   *
   * @throws Exception the exception
   */
  @Test
  public void testLogStatisticsStore() throws Exception {
    LogStatisticsStore store = new LogStatisticsStore();
    store.initialize(LensServerConf.getHiveConf());
    // Do some initialization work
    ByteArrayOutputStream writer = new ByteArrayOutputStream();
    Logger l = (Logger) LoggerFactory.getLogger(MyLoggableLens.class);
    OutputStreamAppender<ILoggingEvent> appender = new OutputStreamAppender<ILoggingEvent>();
    appender.setLayout(new StatisticsLogLayout());
    appender.setContext(l.getLoggerContext());
    appender.setOutputStream(writer);
    appender.setName(MyLoggableLens.class.getCanonicalName());
    appender.start();
    l.addAppender(appender);
    MyLoggableLens sampleEvent = new MyLoggableLens(System.currentTimeMillis());
    store.process(sampleEvent);
    writer.flush();
    l.detachAppender(appender);
    appender.stop();
    ObjectMapper mapper = new ObjectMapper();
    String expected = mapper.writeValueAsString(sampleEvent);
    Assert.assertEquals(new String(writer.toByteArray(), "UTF-8").trim(), expected.trim());
    writer.close();
  }
}
