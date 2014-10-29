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

import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.stats.store.log.PartitionEvent;
import org.apache.lens.server.stats.store.log.StatisticsLogFileScannerTask;
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
import java.util.ArrayList;
import java.util.List;

/**
 * The Class TestStatisticsLogFileScannerTask.
 */
@Test(groups = "unit-test")
public class TestStatisticsLogFileScannerTask {

  /** The f. */
  private File f;

  /** The hidden. */
  private File hidden;

  /**
   * Creates the test log file.
   *
   * @throws Exception
   *           the exception
   */
  @BeforeMethod
  public void createTestLogFile() throws Exception {
    f = new File("/tmp/test.log.2014-08-05-11-28");
    hidden = new File("/tmp/.test.log.2014-08-05-11-28.swp");
    hidden.createNewFile();
    f.createNewFile();
  }

  /**
   * Delete test file.
   *
   * @throws Exception
   *           the exception
   */
  @AfterMethod
  public void deleteTestFile() throws Exception {
    f.delete();
    hidden.delete();
  }

  /**
   * Test scanner.
   *
   * @throws Exception
   *           the exception
   */
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
    LensEventService service = Mockito.mock(LensEventService.class);
    final List<PartitionEvent> events = new ArrayList<PartitionEvent>();
    try {
      Mockito.doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
          events.add((PartitionEvent) invocationOnMock.getArguments()[0]);
          return null;
        }
      }).when(service).notifyEvent(Mockito.any(PartitionEvent.class));
    } catch (Exception e) {
      e.printStackTrace();
    }
    task.setService(service);
    task.run();
    Assert.assertEquals(events.size(), 1);
    PartitionEvent event = events.get(0);
    Assert.assertEquals(event.getEventName(), TestStatisticsLogFileScannerTask.class.getSimpleName());
    Assert.assertEquals(event.getPartMap().size(), 1);
    Assert.assertTrue(event.getPartMap().containsKey("2014-08-05-11-28"));
    Assert.assertEquals(event.getPartMap().get("2014-08-05-11-28"), f.getAbsolutePath());
  }
}
