package org.apache.lens.server.stats;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.lens.server.stats.event.GrillStatistics;
import org.apache.lens.server.stats.event.LoggableGrillStatistics;
import org.apache.lens.server.stats.store.log.LogStatisticsStore;
import org.apache.lens.server.stats.store.log.StatisticsLogLayout;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.StringWriter;

public class TestLogStatisticsStore {


  private static class MyLoggableGrill extends LoggableGrillStatistics {
    public MyLoggableGrill(long eventTime) {
      super(eventTime);
    }

    @Override
    public Table getHiveTable(Configuration conf) {
      return null;
    }

    @Override
    public String getEventId() {
      return "random";
    }
  }

  @Test
  public void testLogStatisticsStore() throws Exception {
    LogStatisticsStore store = new LogStatisticsStore();
    store.initialize(new Configuration());
    //Do some initialization work
    StringWriter writer = new StringWriter();
    Logger l = Logger.getLogger(MyLoggableGrill.class);
    WriterAppender appender = new WriterAppender(new StatisticsLogLayout(), writer);

    appender.setName(MyLoggableGrill.class.getSimpleName());
    l.addAppender(appender);
    MyLoggableGrill sampleEvent = new MyLoggableGrill(System.currentTimeMillis());
    store.process(sampleEvent);
    writer.flush();
    l.removeAppender(appender);
    ObjectMapper mapper = new ObjectMapper();
    String expected = mapper.writeValueAsString(sampleEvent);
    Assert.assertEquals(writer.toString().trim(), expected.trim());
  }
}
