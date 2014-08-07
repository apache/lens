package com.inmobi.grill.server.stats;
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

import com.google.common.eventbus.Subscribe;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.stats.store.log.PartitionEvent;
import com.inmobi.grill.server.stats.store.log.StatisticsLogFileScannerTask;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestStatisticsLogFileScannerTask {


  private File f;
  @BeforeMethod
  public void createTestLogFile() throws Exception {
    f = new File("/tmp/test.log.2014-08-05-11-28");
    f.createNewFile();
  }

  @AfterMethod
  public void deleteTestFile() throws Exception {
    f.delete();
  }


  @Test
  public void testScanner() throws Exception {
    Logger l = Logger.getLogger(TestStatisticsLogFileScannerTask.class);
    FileAppender appender = new FileAppender();
    String logFile = f.getParent() + File.separator + "test.log";
    appender.setFile(logFile);
    appender.setName(TestStatisticsLogFileScannerTask.class.getSimpleName());
    l.addAppender(appender);

    StatisticsLogFileScannerTask task = new StatisticsLogFileScannerTask();
    task.addLogFile(TestStatisticsLogFileScannerTask.class.getName());
    GrillEventService service = Mockito.mock(GrillEventService.class);
    final List<PartitionEvent> events = new ArrayList<PartitionEvent>();
    try {
      Mockito.doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
          events.add((PartitionEvent) invocationOnMock.getArguments()[0]);
          return null;
        }
      }).when(service).notifyEvent(Mockito.any(PartitionEvent.class));
    } catch (Exception e ) {
      e.printStackTrace();
    }
    task.setService(service);
    task.run();
    Assert.assertEquals(events.size(), 1);
    PartitionEvent event = events.get(0);
    Assert.assertEquals(event.getEventName(),
        TestStatisticsLogFileScannerTask.class.getSimpleName());
    Assert.assertTrue(event.getPartMap().containsKey("2014-08-05-11-28"));
    Assert.assertEquals(event.getPartMap().get("2014-08-05-11-28"),
        f.getAbsolutePath());
  }
}
