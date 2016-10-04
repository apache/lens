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

package org.apache.lens.regression.core.helpers;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import javax.ws.rs.core.*;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.ToXMLString;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScheduleResourceHelper extends ServiceManagerHelper {

  public static final String SCHEDULER_BASE_URL = "/scheduler";
  public static final String SCHEDULER_JOBS_URL = SCHEDULER_BASE_URL + "/jobs";
  public static final String SCHEDULER_INSTANCES_URL = SCHEDULER_BASE_URL + "/instances";

  public ScheduleResourceHelper() {
  }

  public ScheduleResourceHelper(String envFileName) {
    super(envFileName);
  }


  public String submitJob(String action, XJob job, String sessionHandleString)
    throws JAXBException, IOException, ParseException, DatatypeConfigurationException {

    MapBuilder map = new MapBuilder("sessionid", sessionHandleString);
    if (action != null) {
      map.put("action", action);
    }

    JAXBElement<XJob> xmlJob = new ObjectFactory().createJob(job);
    GenericEntity<JAXBElement<XJob>> entry = new GenericEntity<JAXBElement<XJob>>(xmlJob){};
    Response response = this.exec("post", SCHEDULER_JOBS_URL, servLens, null, map, MediaType.APPLICATION_XML_TYPE,
        MediaType.APPLICATION_XML, entry);
    AssertUtil.assertSucceededResponse(response);
    SchedulerJobHandle handle =  response.readEntity(SchedulerJobHandle.class);
    return handle.getHandleIdString();
  }

  public String submitJob(XJob job, String sessionHandleString)
    throws JAXBException, IOException, ParseException, DatatypeConfigurationException {
    return submitJob("submit", job, sessionHandleString);
  }

  public String submitNScheduleJob(XJob job, String sessionHandleString)
    throws JAXBException, IOException, ParseException, DatatypeConfigurationException {
    return submitJob("submit_and_schedule", job, sessionHandleString);
  }

  public XJob getXJob(String name, String query, String db, String startTime, String endTime, XFrequencyEnum frequency)
    throws ParseException, DatatypeConfigurationException {

    XJob job = new XJob();
    job.setName(name);
    job.setStartTime(Util.getGregorianCalendar(startTime));
    job.setEndTime(Util.getGregorianCalendar(endTime));
    job.setTrigger(getXTrigger(frequency));
    job.setExecution(getXExecution(query, db));
    return job;
  }

  public XJob getXJob(String name, String query, String db, String startTime, String endTime, String cronExpression)
    throws ParseException, DatatypeConfigurationException {

    XJob job = new XJob();
    job.setName(name);
    job.setStartTime(Util.getGregorianCalendar(startTime));
    job.setEndTime(Util.getGregorianCalendar(endTime));
    job.setTrigger(getXTrigger(cronExpression));
    job.setExecution(getXExecution(query, db));
    return job;
  }

  public XTrigger getXTrigger(XFrequencyEnum frequency){

    XTrigger xTrigger = new XTrigger();
    XFrequency xf = new XFrequency();
    xf.setEnum(frequency);
    xTrigger.setFrequency(xf);
    return xTrigger;
  }

  public XTrigger getXTrigger(String cronExpression){

    XTrigger xTrigger = new XTrigger();
    XFrequency xf = new XFrequency();
    xf.setCronExpression(cronExpression);
    xTrigger.setFrequency(xf);
    return xTrigger;
  }

  public XExecution getXExecution(String query, String db){
    return getXExecution(query, db, null, null);
  }

  public XExecution getXExecution(String query, String db, List<MapType> queryConfig, List<MapType> sessionConfig){
    XExecution execution = new XExecution();

    XJobQuery xJobQuery = new XJobQuery();
    xJobQuery.setQuery(query);
    if (queryConfig != null){
      xJobQuery.withConf(queryConfig);
    }

    XSessionType sessionType = new XSessionType();
    if (db == null){
      db = getCurrentDB();
    }
    sessionType.setDb(db);
    if (sessionConfig != null){
      sessionType.withConf(sessionConfig);
    }

    execution.setQuery(xJobQuery);
    execution.setSession(sessionType);
    return execution;
  }

  public XJob getJobDefinition(String jobHandle, String sessionId, MediaType inputMedia, String outputMedia){
    MapBuilder map = new MapBuilder("sessionid", sessionId);
    Response response = this.exec("get", SCHEDULER_JOBS_URL + "/" + jobHandle , servLens, null, map, inputMedia,
        outputMedia);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(XJob.class);
  }

  public XJob getJobDefinition(String jobHandle, String sessionId){
    return getJobDefinition(jobHandle, sessionId, MediaType.APPLICATION_XML_TYPE, MediaType.APPLICATION_XML);
  }

  public APIResult deleteJob(String jobHandle, String sessionId){
    MapBuilder map = new MapBuilder("sessionid", sessionId);
    Response response = this.exec("delete", SCHEDULER_JOBS_URL + "/" + jobHandle , servLens, null, map, null,
        MediaType.APPLICATION_JSON);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(APIResult.class);
  }

  public APIResult updateJob(XJob job, String jobHandle, String sessionHandleString)
    throws JAXBException, IOException, ParseException, DatatypeConfigurationException {

    MapBuilder map = new MapBuilder("sessionid", sessionHandleString);
    JAXBElement<XJob> xmlJob = new ObjectFactory().createJob(job);

    Response response = this.exec("put", SCHEDULER_JOBS_URL + "/" + jobHandle, servLens, null, map,
        MediaType.APPLICATION_XML_TYPE, MediaType.APPLICATION_XML, ToXMLString.toString(xmlJob));
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(APIResult.class);
  }

  public APIResult updateJob(String jobHandle, String action, String sessionHandleString)
    throws JAXBException, IOException, ParseException, DatatypeConfigurationException {

    MapBuilder map = new MapBuilder("sessionid", sessionHandleString, "action", action);
    Response response = this.exec("post", SCHEDULER_JOBS_URL + "/" + jobHandle, servLens, null, map);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(APIResult.class);
  }

  public SchedulerJobInfo getJobDetails(String jobHandle, String sessionHandleString){

    MapBuilder map = new MapBuilder("sessionid", sessionHandleString);
    Response response = this.exec("get", SCHEDULER_JOBS_URL + "/" + jobHandle + "/info", servLens, null, map,
        MediaType.APPLICATION_XML_TYPE, MediaType.APPLICATION_XML);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(SchedulerJobInfo.class);
  }


  public SchedulerJobState getJobStatus(String jobHandle, String sessionHandleString){
    SchedulerJobInfo jobInfo = getJobDetails(jobHandle, sessionHandleString);
    return jobInfo.getJobState();
  }

  public SchedulerJobState getJobStatus(String jobHandle){
    SchedulerJobInfo jobInfo = getJobDetails(jobHandle, sessionHandleString);
    return jobInfo.getJobState();
  }

  public List<SchedulerJobInstanceInfo> getAllInstancesOfJob(String jobHandle, String numResults, String sessionId)
    throws JAXBException, IOException, ParseException, DatatypeConfigurationException {

    MapBuilder map = new MapBuilder("sessionid", sessionId, "numResults", numResults);
    Response response = this.exec("get", SCHEDULER_JOBS_URL + "/" + jobHandle + "/instances", servLens, null, map);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(new GenericType<List<SchedulerJobInstanceInfo>>(){});
  }

  public SchedulerJobInstanceInfo getInstanceDetails(String instanceHandle, String sessionId)
    throws JAXBException, IOException, ParseException, DatatypeConfigurationException {

    MapBuilder map = new MapBuilder("sessionid", sessionId);
    Response response = this.exec("get", SCHEDULER_INSTANCES_URL + "/" + instanceHandle , servLens, null, map);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(SchedulerJobInstanceInfo.class);
  }

  public APIResult updateInstance(String instanceHandle, String action, String sessionId)
    throws JAXBException, IOException, ParseException, DatatypeConfigurationException {

    MapBuilder map = new MapBuilder("sessionid", sessionId, "action", action);
    Response response = this.exec("post", SCHEDULER_INSTANCES_URL + "/" + instanceHandle , servLens, null, map);
    AssertUtil.assertSucceededResponse(response);
    return response.readEntity(APIResult.class);
  }
}
