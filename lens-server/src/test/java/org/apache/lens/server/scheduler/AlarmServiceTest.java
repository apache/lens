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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lens.server.scheduler;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.*;

import org.apache.lens.api.scheduler.SchedulerJobHandle;
import org.apache.lens.api.scheduler.XFrequency;
import org.apache.lens.api.scheduler.XFrequencyEnum;
import org.apache.lens.server.EventServiceImpl;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.events.SchedulerAlarmEvent;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * Tests for AlarmService.
 */
@Slf4j
@Test(groups = "unit-test")
public class AlarmServiceTest {

  private static List<SchedulerAlarmEvent> events = new LinkedList<>();
  private static volatile int counter = 0;
  private final LensEventListener<SchedulerAlarmEvent> alarmEventListener;
  private AlarmService alarmService;
  private EventServiceImpl eventService;

  {
    alarmEventListener = new LensEventListener<SchedulerAlarmEvent>() {

      @Override
      public void onEvent(SchedulerAlarmEvent event) {
        events.add(event);
      }
    };
  }

  @BeforeMethod
  public void initializeEventsList() {
    events = new LinkedList<>();
  }

  @BeforeClass
  public void setUp() {
    System.setProperty(LensConfConstants.CONFIG_LOCATION, "target/test-classes/");
    LensServices.get().init(LensServerConf.getHiveConf());
    LensServices.get().start();
    eventService = LensServices.get().getService(LensEventService.NAME);
    assertNotNull(eventService);
    alarmService = LensServices.get().getService(AlarmService.NAME);
    assertNotNull(alarmService);
    eventService.addListenerForType(alarmEventListener, SchedulerAlarmEvent.class);
  }

  /**
   * This test generally tests the basic understanding and assumptions about quartz framework with a sample job.
   *
   * @throws SchedulerException
   * @throws InterruptedException
   */
  @Test
  public void testCoreExecution() throws SchedulerException, InterruptedException {
    SchedulerFactory sf = new StdSchedulerFactory();
    Scheduler scheduler = sf.getScheduler();
    scheduler.start();
    // prepare a sample Job
    JobDetail job = JobBuilder.newJob(TestJob.class).withIdentity("random", "group").build();
    scheduler.scheduleJob(job, getPastPerSecondsTrigger());
    Thread.sleep(2000);
    Assert.assertEquals(counter, 10);
    scheduler.deleteJob(job.getKey());
  }

  private CalendarIntervalScheduleBuilder getPerSecondCalendar() {
    return CalendarIntervalScheduleBuilder.calendarIntervalSchedule().withInterval(1, DateBuilder.IntervalUnit.SECOND)
        .withMisfireHandlingInstructionFireAndProceed();
  }

  private Trigger getPastPerSecondsTrigger() {
    CalendarIntervalScheduleBuilder scheduleBuilder = getPerSecondCalendar();
    Date start = new Date();
    start = new Date(start.getTime() - 10000);
    Date end = new Date();

    return TriggerBuilder.newTrigger().withIdentity("trigger1", "group1").startAt(start).endAt(end)
        .withSchedule(scheduleBuilder).build();
  }

  @Test
  public void testAlarmServiceEnums() throws InterruptedException, LensException {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-mm-dd");
    DateTime start = formatter.parseDateTime("2016-03-03");
    DateTime end = formatter.parseDateTime("2016-03-06");
    SchedulerJobHandle jobHandle = new SchedulerJobHandle(UUID.randomUUID());
    XFrequency frequency = new XFrequency();
    frequency.setEnum(XFrequencyEnum.DAILY);
    alarmService.schedule(start, end, frequency, jobHandle.toString());
    Thread.sleep(2000); // give chance to the event listener to process the data
    int count = 0;
    for (SchedulerAlarmEvent event : events) {
      if (event.getJobHandle().equals(jobHandle)) {
        count++;
      }
    }
    // 3 scheduled events, and one expired event.
    Assert.assertEquals(count, 4);
    DateTime expectedDate = start;
    Set<DateTime> actualSet = new HashSet<>();
    for (SchedulerAlarmEvent e : events) {
      actualSet.add(e.getNominalTime());
    }

    for (int i = 0; i < actualSet.size(); i++) {
      Assert.assertTrue(actualSet.contains(expectedDate));
      expectedDate = expectedDate.plusDays(1);
    }
  }

  @Test
  public void testAlarmServiceCronExpressions() throws InterruptedException, LensException {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-mm-dd");
    DateTime start = formatter.parseDateTime("2016-03-03");
    DateTime end = formatter.parseDateTime("2016-03-06");
    SchedulerJobHandle jobHandle = new SchedulerJobHandle(UUID.randomUUID());
    System.out.println("jobHandle = " + jobHandle);
    XFrequency frequency = new XFrequency();
    frequency.setCronExpression("0 0 12 * * ?");
    alarmService.schedule(start, end, frequency, jobHandle.toString());
    Thread.sleep(1000);
    alarmService.unSchedule(jobHandle);
    // Assert that the events are fired and at per second interval.
    assertTrue(events.size() > 1);
  }

  @PersistJobDataAfterExecution
  @DisallowConcurrentExecution
  public static class TestJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      AlarmServiceTest.counter++;
    }
  }
}
