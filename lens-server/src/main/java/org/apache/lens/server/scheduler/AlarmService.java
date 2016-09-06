/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lens.server.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.api.scheduler.SchedulerJobHandle;
import org.apache.lens.api.scheduler.XFrequency;
import org.apache.lens.api.scheduler.XFrequencyEnum;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensService;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.events.SchedulerAlarmEvent;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.error.LensSchedulerErrorCode;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;

import org.joda.time.DateTime;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * This service is used primarily by Scheduler to get alarm notifications for scheduled queries.
 * <p>
 * As a schedule this service accepts start time, frequency, end time and timeZone. It also requires the
 * {@link SchedulerJobHandle} which it sends as part of the
 * {@link org.apache.lens.server.api.events.SchedulerAlarmEvent} to inform the scheduler about the job for which
 * job the notification has been generated.
 */
@Slf4j
public class AlarmService extends AbstractService implements LensService {

  public static final String NAME = "alarm";

  public static final String LENS_JOBS = "LensJobs";
  public static final String ALARM_SERVICE = "AlarmService";
  private static final String JOB_HANDLE = "jobHandle";
  private static final String SHOULD_WAIT = "shouldWaitForProcessing";
  @Getter
  @Setter
  private boolean shouldWaitForScheduleEventProcessing = false;

  private Scheduler scheduler;

  /**
   * True if the service started properly and is running fine, false otherwise.
   */
  private boolean isHealthy = true;

  /**
   * Contains the reason if service is not healthy.
   */
  private String healthCause;

  /**
   * Creates a new instance of AlarmService.
   *
   */
  public AlarmService() {
    super(NAME);
  }

  @Override
  public HealthStatus getHealthStatus() {
    return isHealthy
           ? new HealthStatus(isHealthy, "Alarm service is healthy.")
           : new HealthStatus(isHealthy, healthCause);
  }

  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    try {
      this.scheduler = StdSchedulerFactory.getDefaultScheduler();
    } catch (SchedulerException e) {
      isHealthy = false;
      healthCause = "Failed to initialize the Quartz Scheduler for AlarmService.";
      log.error(healthCause, e);
      throw new IllegalStateException("Could not initialize the Alarm Service", e);
    }
  }

  @Override
  public synchronized void start() {
    try {
      scheduler.start();
      log.info("Alarm service started successfully!");
    } catch (SchedulerException e) {
      isHealthy = false;
      healthCause = "Failed to start the Quartz Scheduler for AlarmService.";
      log.error(healthCause, e);
      throw new IllegalStateException("Could not start the Alarm service", e);
    }
  }

  @Override
  public synchronized void stop() {
    try {
      scheduler.standby();
      log.info("Alarm Service stopped successfully.");
    } catch (SchedulerException e) {
      log.error("Failed to shut down the Quartz Scheduler for AlarmService.", e);
    }
  }

  public List<JobExecutionContext> getCurrentlyExecutingJobs() {
    try {
      return scheduler.getCurrentlyExecutingJobs();
    } catch (SchedulerException e) {
      log.error("Failed to get currently executing jobs");
    }
    return new ArrayList<>();
  }

  /**
   * This method can be used by any consumer who wants to receive notifications during a time range at a given
   * frequency.
   * <p>
   * This method is intended to be used by LensScheduler to subscribe for time based notifications to schedule queries.
   * On receiving a job to be scheduled LensScheduler will subscribe to all triggers required for the job, including
   * AlarmService for time based triggers.
   *
   * @param start     start time for notifications
   * @param end       end time for notifications
   * @param frequency Frequency to determine the frequency at which notification should be sent.
   * @param jobHandle Must be a unique jobHanlde across all consumers
   */
  public void schedule(DateTime start, DateTime end, XFrequency frequency, String jobHandle) throws LensException {
    // accept the schedule and then keep on sending the notifications for that schedule
    JobDataMap map = new JobDataMap();
    map.put(JOB_HANDLE, jobHandle);
    map.put(SHOULD_WAIT, shouldWaitForScheduleEventProcessing);
    JobDetail job = JobBuilder.newJob(LensJob.class).withIdentity(jobHandle, LENS_JOBS).usingJobData(map).build();

    Trigger trigger;
    if (frequency.getEnum() != null) { //for enum expression:  create a trigger using calendar interval
      CalendarIntervalScheduleBuilder scheduleBuilder = CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
        .withInterval(getTimeInterval(frequency.getEnum()), getTimeUnit(frequency.getEnum()))
        .withMisfireHandlingInstructionIgnoreMisfires();
      trigger = TriggerBuilder.newTrigger().withIdentity(jobHandle, ALARM_SERVICE).startAt(start.toDate())
        .endAt(end.toDate()).withSchedule(scheduleBuilder).build();
    } else { // for cron expression create a cron trigger
      trigger = TriggerBuilder.newTrigger().withIdentity(jobHandle, ALARM_SERVICE).startAt(start.toDate())
        .endAt(end.toDate()).withSchedule(CronScheduleBuilder.cronSchedule(frequency.getCronExpression())
          .withMisfireHandlingInstructionIgnoreMisfires()).build();
    }

    // Tell quartz to run the job using our trigger
    try {
      scheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      log.error("Error scheduling job with jobHandle: {}", jobHandle);
      throw new LensException(LensSchedulerErrorCode.FAILED_ALARM_SERVICE_OPERATION.getLensErrorInfo(), e, "schedule",
        jobHandle);
    }
  }

  private int getTimeInterval(XFrequencyEnum frequencyEnum) {
    // since quarterly is not supported natively, we express it as 3 months
    return frequencyEnum == XFrequencyEnum.QUARTERLY ? 3 : 1;
  }

  // Maps the timeunit in entity specification to the one in Quartz DateBuilder
  private DateBuilder.IntervalUnit getTimeUnit(XFrequencyEnum frequencyEnum) {
    switch (frequencyEnum) {

    case DAILY:
      return DateBuilder.IntervalUnit.DAY;

    case WEEKLY:
      return DateBuilder.IntervalUnit.WEEK;

    case MONTHLY:
      return DateBuilder.IntervalUnit.MONTH;

    case QUARTERLY:
      return DateBuilder.IntervalUnit.MONTH;

    case YEARLY:
      return DateBuilder.IntervalUnit.YEAR;

    default:
      throw new IllegalArgumentException("Invalid frequency enum expression: " + frequencyEnum.name());
    }
  }

  public boolean unSchedule(SchedulerJobHandle jobHandle) throws LensException {
    // stop sending notifications for this job handle
    try {
      return scheduler.deleteJob(JobKey.jobKey(jobHandle.getHandleIdString(), LENS_JOBS));
    } catch (SchedulerException e) {
      log.error("Failed to remove alarm triggers for job with jobHandle: {}", jobHandle);
      throw new LensException(LensSchedulerErrorCode.FAILED_ALARM_SERVICE_OPERATION.getLensErrorInfo(), e, "unschedule",
        jobHandle.getHandleIdString());
    }
  }

  public boolean checkExists(SchedulerJobHandle handle) throws LensException {
    try {
      return scheduler.checkExists(JobKey.jobKey(handle.getHandleIdString(), LENS_JOBS));
    } catch (SchedulerException e) {
      log.error("Failed to check the job with jobHandle: {}", handle);
      return false;
    }
  }

  public void pauseJob(SchedulerJobHandle jobHandle) throws LensException {
    try {
      scheduler.pauseJob(JobKey.jobKey(jobHandle.getHandleIdString(), LENS_JOBS));
    } catch (SchedulerException e) {
      log.error("Failed to pause alarm triggers for job with jobHandle: {}", jobHandle);
      throw new LensException(LensSchedulerErrorCode.FAILED_ALARM_SERVICE_OPERATION.getLensErrorInfo(), e, "pause",
        jobHandle.getHandleIdString());
    }
  }

  public void resumeJob(SchedulerJobHandle jobHandle) throws LensException {
    try {
      scheduler.resumeJob(JobKey.jobKey(jobHandle.getHandleIdString(), LENS_JOBS));
    } catch (SchedulerException e) {
      log.error("Failed to resume alarm triggers for job with jobHandle: {}", jobHandle);
      throw new LensException(LensSchedulerErrorCode.FAILED_ALARM_SERVICE_OPERATION.getLensErrorInfo(), e, "resume",
        jobHandle.getHandleIdString());
    }
  }

  public static class LensJob implements Job {

    private void notifyEventService(SchedulerAlarmEvent alarmEvent, boolean shouldWait)
      throws LensException, InterruptedException {
      LensEventService eventService = LensServices.get().getService(LensEventService.NAME);
      if (shouldWait) {
        eventService.notifyEventSync(alarmEvent);
      } else {
        eventService.notifyEvent(alarmEvent);
      }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
      JobDataMap data = jobExecutionContext.getMergedJobDataMap();
      DateTime nominalTime = new DateTime(jobExecutionContext.getScheduledFireTime());
      SchedulerJobHandle jobHandle = SchedulerJobHandle.fromString(data.getString(JOB_HANDLE));
      boolean shouldWait = data.getBoolean(SHOULD_WAIT);
      SchedulerAlarmEvent alarmEvent = new SchedulerAlarmEvent(jobHandle, nominalTime,
        SchedulerAlarmEvent.EventType.SCHEDULE, null);
      try {
        notifyEventService(alarmEvent, shouldWait);
        if (jobExecutionContext.getNextFireTime() == null) {
          SchedulerAlarmEvent expireEvent = (new SchedulerAlarmEvent(jobHandle, nominalTime,
            SchedulerAlarmEvent.EventType.EXPIRE, null));
          notifyEventService(expireEvent, shouldWait);
        }
      } catch (LensException e) {
        log.error("Failed to notify SchedulerAlarmEvent for jobHandle: {} and scheduleTime: {}",
          jobHandle.getHandleIdString(), nominalTime.toString());
        throw new JobExecutionException("Failed to notify alarmEvent", e);
      } catch (InterruptedException e) {
        log.error("Job execution tread interrupted", e);
      }
    }
  }
}
