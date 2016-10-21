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
package org.apache.lens.server.scheduler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBElement;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.ToXMLString;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * SchedulerDAO class defines scheduler store operations.
 */
@Slf4j
public class SchedulerDAO {
  private Configuration conf;
  private SchedulerDBStore store;

  public SchedulerDAO(Configuration conf) throws LensException {
    this.conf = conf;
    try {
      Class dbStoreClass = Class
        .forName(conf.get(LensConfConstants.SCHEDULER_STORE_CLASS, SchedulerHsqlDBStore.class.getName()));
      this.store = (SchedulerDBStore) dbStoreClass.newInstance();
      this.store.init(UtilityMethods.getDataSourceFromConfForScheduler(conf));
      this.store.createJobTable();
      this.store.createJobInstanceTable();
      this.store.createJobInstanceRunTable();
    } catch (SQLException e) {
      // If tables are not created, the DAO operations will fail at runtime.
      // The APIs will fail with Internal Server Error.
      log.error("Error creating job tables", e);
    } catch (ClassNotFoundException e) {
      log.error("No class found ", e);
      throw new LensException("No class found ", e);
    } catch (InstantiationException | IllegalAccessException e) {
      log.error("Illegal access exception", e);
      throw new LensException("Illegal access exception ", e);
    }
  }

  /**
   * Saves the SchedulerJobInfo object into the store.
   *
   * @param jobInfo object
   * @return the number of records stored
   */
  public int storeJob(SchedulerJobInfo jobInfo) {
    try {
      return store.insertIntoJobTable(jobInfo);
    } catch (SQLException e) {
      log.error("Error while storing the jobInfo for {}", jobInfo.getId().getHandleIdString(), e);
      return 0;
    }
  }

  /**
   * Fetches the SchedulerJobInfo object corresponding to handle id.
   *
   * @param id: Job handle id.
   * @return SchedulerJobInfo
   */
  public SchedulerJobInfo getSchedulerJobInfo(SchedulerJobHandle id) {
    try {
      return store.getSchedulerJobInfo(id.getHandleIdString());
    } catch (SQLException e) {
      log.error("Error while getting the job detail for {}", id.getHandleIdString(), e);
      return null;
    }
  }

  /**
   * Gets the job definition from the store
   *
   * @param id : Job handle id.
   * @return XJob definition
   */
  public XJob getJob(SchedulerJobHandle id) {
    try {
      return store.getJob(id.getHandleIdString());
    } catch (SQLException e) {
      log.error("Error while getting the job for {}", id.getHandleIdString(), e);
      return null;
    }
  }

  /**
   * Get the user of the job who submitted the job.
   *
   * @param id
   * @return
   */
  public String getUser(SchedulerJobHandle id) {
    try {
      return store.getUser(id.getHandleIdString());
    } catch (SQLException e) {
      log.error("Error while getting the user for the job with handle {}", id.getHandleIdString(), e);
      return null;
    }
  }

  /**
   * Gets the Job status
   *
   * @param id : Job handle id.
   * @return SchedulerJobStatus of the job.
   */
  public SchedulerJobState getJobState(SchedulerJobHandle id) {
    try {
      return store.getJobState(id.getHandleIdString());
    } catch (SQLException e) {
      log.error("Error while getting the job status for {}", id.getHandleIdString(), e);
      return null;
    }
  }

  /**
   * Updates the job definition from the new SchedulerJobInfo
   *
   * @param info: Updated info object.
   * @return number of rows updated.
   */
  public int updateJob(SchedulerJobInfo info) {
    try {
      return store.updateJob(info.getId().getHandleIdString(), info.getJob(), info.getModifiedOn());
    } catch (SQLException e) {
      log.error("Error while updating job for {}", info.getId().getHandleIdString(), e);
      return 0;
    }
  }

  /**
   * Updates the job status form the new SchedulerJobInfo
   *
   * @param info: Updated info objects
   * @return number of rows updated.
   */
  public int updateJobStatus(SchedulerJobInfo info) {
    try {
      return store.updateJobStatus(info.getId().getHandleIdString(), info.getJobState().name(), info.getModifiedOn());
    } catch (SQLException e) {
      log.error("Error while updating job status for {}", info.getId().getHandleIdString(), e);
      return 0;
    }
  }

  public int storeJobInstance(SchedulerJobInstanceInfo instanceInfo) {
    try {
      return store.insertIntoJobInstanceTable(instanceInfo);
    } catch (SQLException e) {
      log.error("Error while storing job instance for {}", instanceInfo.getId(), e);
      return 0;
    }
  }

  public int storeJobInstanceRun(SchedulerJobInstanceRun instanceRun) {
    try {
      return store.insertIntoJobInstanceRunTable(instanceRun);
    } catch (SQLException e) {
      log.error("Error while storing job instance run for {} with run id {} ",
        instanceRun.getHandle().getHandleIdString(), instanceRun.getRunId(), e);

      return 0;
    }
  }

  /**
   * Gets the SchedulerJobInstanceInfo corresponding instance handle id.
   *
   * @param id : Job instance id
   * @return ShedulerJobInstanceInfo
   */
  public SchedulerJobInstanceInfo getSchedulerJobInstanceInfo(SchedulerJobInstanceHandle id) {
    try {
      return store.getJobInstanceInfo(id.getHandleIdString());
    } catch (SQLException e) {
      log.error("Error while getting the job instance info for {}", id.getHandleIdString(), e);
      return null;
    }
  }

  /**
   * Updates the instance status
   *
   * @param instanceRun : instance Run object
   * @return number of rows updated.
   */
  public int updateJobInstanceRun(SchedulerJobInstanceRun instanceRun) {
    try {
      return store.updateJobInstanceRun(instanceRun);
    } catch (SQLException e) {
      log.error("Error while updating the job instance status for {} and run id {}",
        instanceRun.getHandle().getHandleIdString(), instanceRun.getRunId(), e);
      return 0;
    }
  }

  /**
   * Gets all the instance handle id for a job.
   *
   * @param id: Job handle id.
   * @return List of instance handles.
   */
  public List<SchedulerJobInstanceInfo> getJobInstances(SchedulerJobHandle id) {
    // TODO: Add number of results to be fetched
    try {
      return store.getAllJobInstances(id.getHandleIdString());
    } catch (SQLException e) {
      log.error("Error while getting instances of a job with id {}", id.getHandleIdString(), e);
      return new ArrayList<>();
    }
  }

  /**
   * Gets all jobs which match the filter requirements.
   *
   * @param username  : User name of the job
   * @param startTime : Created on should be greater than this start time.
   * @param endTime   : Created on should be less than the end time.
   * @param jobStates : Multiple states of the jobs
   * @return List of Job handles
   */
  public List<SchedulerJobHandle> getJobs(String username, Long startTime, Long endTime,
    SchedulerJobState... jobStates) {
    try {
      return store.getJobs(username, jobStates == null ? new SchedulerJobState[] {} : jobStates, startTime, endTime);
    } catch (SQLException e) {
      log.error("Error while getting jobs ", e);
      return new ArrayList<>();
    }
  }

  /**
   * Get the handle given the job name.
   *
   * @param jobName
   * @return
   */
  public List<SchedulerJobHandle> getJob(String jobName) {
    try {
      return store.getJobsByName(jobName);
    } catch (SQLException e) {
      log.error("Error while getting jobs ", e);
      return new ArrayList<>();
    }
  }

  /**
   * Get all instances matching one of the states
   *
   * @param states States to be consider for filter
   * @return A list of SchedulerJobInstanceRun
   */
  public List<SchedulerJobInstanceRun> getInstanceRuns(SchedulerJobInstanceState... states) {
    try {
      return store.getInstanceRuns(states);
    } catch (SQLException e) {
      log.error("Error while getting jobs ", e);
      return new ArrayList<>();
    }
  }

  public abstract static class SchedulerDBStore {
    protected static final String JOB_TABLE = "job_table";
    protected static final String JOB_INSTANCE_TABLE = "job_instance_table";
    protected static final String JOB_INSTANCE_RUN_TABLE = "job_instance_run_table";
    protected static final String COLUMN_ID = "id";
    protected static final String COLUMN_RUN_ID = "runid";
    protected static final String COLUMN_JOB = "job";
    protected static final String COLUMN_USER = "username";
    protected static final String COLUMN_STATUS = "status";
    protected static final String COLUMN_CREATED_ON = "createdon";
    protected static final String COLUMN_SCHEDULE_TIME = "scheduledtime";
    protected static final String COLUMN_MODIFIED_ON = "modifiedon";
    protected static final String COLUMN_JOB_ID = "jobid";
    protected static final String COLUMN_SESSION_HANDLE = "sessionhandle";
    protected static final String COLUMN_START_TIME = "starttime";
    protected static final String COLUMN_END_TIME = "endtime";
    protected static final String COLUMN_RESULT_PATH = "resultpath";
    protected static final String COLUMN_QUERY_HANDLE = "queryhandle";
    protected static final String COLUMN_JOB_NAME = "jobname";
    protected QueryRunner runner;
    protected ObjectFactory jobFactory = new ObjectFactory();
    // Generic multiple row handler for the fetch query.
    private ResultSetHandler<List<Object[]>> multipleRowsHandler = new ResultSetHandler<List<Object[]>>() {
      @Override
      public List<Object[]> handle(ResultSet resultSet) throws SQLException {
        List<Object[]> output = new ArrayList<>();
        while (resultSet.next()) {
          ResultSetMetaData meta = resultSet.getMetaData();
          int cols = meta.getColumnCount();
          Object[] result = new Object[cols];
          for (int i = 0; i < cols; i++) {
            result[i] = resultSet.getObject(i + 1);
          }
          output.add(result);
        }
        return output;
      }
    };

    /**
     * Init the store.
     *
     * @param ds
     */
    public void init(BasicDataSource ds) {
      runner = new QueryRunner(ds);
    }

    /**
     * Creates the job table
     *
     * @throws SQLException
     */
    public abstract void createJobTable() throws SQLException;

    /**
     * Creates the job instance table
     *
     * @throws SQLException
     */
    public abstract void createJobInstanceTable() throws SQLException;

    /**
     * Creates the job instance run table
     *
     * @throws SQLException
     */
    public abstract void createJobInstanceRunTable() throws SQLException;

    /**
     * Inserts the Job info object into job table
     *
     * @param jobInfo
     * @return number of rows inserted.
     * @throws SQLException
     */
    public int insertIntoJobTable(SchedulerJobInfo jobInfo) throws SQLException {
      String insertSQL = "INSERT INTO " + JOB_TABLE + " VALUES(?,?,?,?,?,?,?)";
      JAXBElement<XJob> xmlJob = jobFactory.createJob(jobInfo.getJob());
      return runner.update(insertSQL, jobInfo.getId().toString(), ToXMLString.toString(xmlJob), jobInfo.getUserName(),
        jobInfo.getJobState().name(), jobInfo.getCreatedOn(), jobInfo.getModifiedOn(), jobInfo.getJob().getName());
    }

    /**
     * Inserts the job instance info object into job instance table
     *
     * @param instanceInfo
     * @return number of rows inserted.
     * @throws SQLException
     */
    public int insertIntoJobInstanceTable(SchedulerJobInstanceInfo instanceInfo) throws SQLException {
      String insertSQL = "INSERT INTO " + JOB_INSTANCE_TABLE + " VALUES(?,?,?)";
      return runner
        .update(insertSQL, instanceInfo.getId().getHandleIdString(), instanceInfo.getJobId().getHandleIdString(),
          instanceInfo.getScheduleTime());
    }

    public int insertIntoJobInstanceRunTable(SchedulerJobInstanceRun instanceRun) throws SQLException {
      String insetSQL = "INSERT INTO " + JOB_INSTANCE_RUN_TABLE + " VALUES(?,?,?,?,?,?,?,?)";
      return runner.update(insetSQL, instanceRun.getHandle().getHandleIdString(), instanceRun.getRunId(),
        instanceRun.getSessionHandle() == null ? "" : instanceRun.getSessionHandle().toString(),
        instanceRun.getStartTime(), instanceRun.getEndTime(), instanceRun.getResultPath(),
        instanceRun.getQueryHandle() == null ? "" : instanceRun.getQueryHandle().getHandleIdString(),
        instanceRun.getInstanceState().name());
    }

    /**
     * Gets the Job info object
     *
     * @param idStr
     * @return SchedulerJobInfo object corresponding to the job handle.
     * @throws SQLException
     */
    public SchedulerJobInfo getSchedulerJobInfo(String idStr) throws SQLException {
      String fetchSQL = "SELECT * FROM " + JOB_TABLE + " WHERE " + COLUMN_ID + "=?";
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler, idStr);
      if (result.size() == 0) {
        return null;
      } else {
        Object[] jobInfo = result.get(0);
        SchedulerJobHandle id = SchedulerJobHandle.fromString((String) jobInfo[0]);
        XJob xJob = ToXMLString.valueOf((String) jobInfo[1], ObjectFactory.class);
        String userName = (String) jobInfo[2];
        String status = (String) jobInfo[3];
        long createdOn = (Long) jobInfo[4];
        long modifiedOn = (Long) jobInfo[5];
        SchedulerJobState jobState = SchedulerJobState.valueOf(status);
        return new SchedulerJobInfo(id, xJob, userName, jobState, createdOn, modifiedOn);
      }
    }

    /**
     * Gets the job definition
     *
     * @param id
     * @return Xjob corresponding to the job handle
     * @throws SQLException
     */
    public XJob getJob(String id) throws SQLException {
      String fetchSQL = "SELECT " + COLUMN_JOB + " FROM " + JOB_TABLE + " WHERE " + COLUMN_ID + "=?";
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler, id);
      if (result.size() == 0) {
        return null;
      } else {
        return ToXMLString.valueOf((String) result.get(0)[0], ObjectFactory.class);
      }
    }

    /**
     * Get the job handles given the job name
     *
     * @param jobname
     * @return A list of handles
     * @throws SQLException
     */
    public List<SchedulerJobHandle> getJobsByName(String jobname) throws SQLException {
      String fetchSQL = "SELCET " + COLUMN_ID + " FROM " + JOB_TABLE + " WHERE " + COLUMN_JOB_NAME + "=?";
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler, jobname);
      List<SchedulerJobHandle> resOut = new ArrayList<>();
      for (int i = 0; i < result.size(); i++) {
        Object[] row = result.get(i);
        resOut.add(SchedulerJobHandle.fromString((String) row[0]));
      }
      return resOut;
    }

    /**
     * Gets the job status
     *
     * @param id
     * @return SchedulerJobStatus
     * @throws SQLException
     */
    public SchedulerJobState getJobState(String id) throws SQLException {
      String fetchSQL = "SELECT " + COLUMN_STATUS + " FROM " + JOB_TABLE + " WHERE " + COLUMN_ID + "=?";
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler, id);
      if (result.size() == 0) {
        return null;
      } else {
        return SchedulerJobState.valueOf((String) result.get(0)[0]);
      }
    }

    /**
     * Gets all the jobs which match the filter requirements.
     *
     * @param username
     * @param states
     * @param starttime
     * @param endtime
     * @return the list of job handles.
     * @throws SQLException
     */
    public List<SchedulerJobHandle> getJobs(String username, SchedulerJobState[] states, Long starttime, Long endtime)
      throws SQLException {
      String whereClause = "";
      if (username != null && !username.isEmpty()) {
        whereClause += ((whereClause.isEmpty()) ? " WHERE " : " AND ") + COLUMN_USER + " = '" + username + "'";
      }
      if (states.length > 0) {
        whereClause += ((whereClause.isEmpty()) ? " WHERE " : " AND ") + COLUMN_STATUS + " IN (";
        String internalWhere = "";
        for (SchedulerJobState state : states) {
          internalWhere += ((internalWhere.isEmpty()) ? "'" : " , '") + state + "'";
        }
        whereClause += internalWhere;
        whereClause += ")";
      }
      if (starttime != null && starttime > 0) {
        whereClause += ((whereClause.isEmpty()) ? " WHERE " : " AND ") + COLUMN_CREATED_ON + " >= " + starttime;
      }
      if (endtime != null && endtime > 0) {
        whereClause += ((whereClause.isEmpty()) ? " WHERE " : " AND ") + COLUMN_CREATED_ON + " < " + endtime;
      }

      String fetchSQL = "SELECT " + COLUMN_ID + " FROM " + JOB_TABLE + whereClause;
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler);
      List<SchedulerJobHandle> resOut = new ArrayList<>();
      for (int i = 0; i < result.size(); i++) {
        Object[] row = result.get(i);
        resOut.add(SchedulerJobHandle.fromString((String) row[0]));
      }
      return resOut;
    }

    /**
     * Update the XJob into the job table
     *
     * @param id
     * @param job
     * @param modifiedOn
     * @return the number of rows updated
     * @throws SQLException
     */
    public int updateJob(String id, XJob job, long modifiedOn) throws SQLException {
      String updateSQL =
        "UPDATE " + JOB_TABLE + " SET " + COLUMN_JOB + "=?, " + COLUMN_MODIFIED_ON + "=? " + " WHERE " + COLUMN_ID
          + "=?";
      JAXBElement<XJob> xmlJob = jobFactory.createJob(job);
      return runner.update(updateSQL, ToXMLString.toString(xmlJob), modifiedOn, id);
    }

    /**
     * Updates the job status into the job table
     *
     * @param id
     * @param status
     * @param modifiedOn
     * @return number of rows updated.
     * @throws SQLException
     */
    public int updateJobStatus(String id, String status, long modifiedOn) throws SQLException {
      String updateSQL =
        "UPDATE " + JOB_TABLE + " SET " + COLUMN_STATUS + "=?, " + COLUMN_MODIFIED_ON + "=? " + " WHERE " + COLUMN_ID
          + "=?";
      return runner.update(updateSQL, status, modifiedOn, id);
    }

    /**
     * Gets the Job instance info corresponding to handle id.
     *
     * @param idStr
     * @return SchedulerJobInstanceInfo
     * @throws SQLException
     */
    public SchedulerJobInstanceInfo getJobInstanceInfo(String idStr) throws SQLException {
      String fetchSQL = "SELECT * FROM " + JOB_INSTANCE_TABLE + " WHERE " + COLUMN_ID + "=?";
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler, idStr);
      if (result.size() == 0) {
        return null;
      } else {
        Object[] instanceInfo = result.get(0);
        return parseSchedulerInstance(instanceInfo);
      }
    }

    private SchedulerJobInstanceInfo parseSchedulerInstance(Object[] instanceInfo) throws SQLException {
      SchedulerJobInstanceHandle id = SchedulerJobInstanceHandle.fromString((String) instanceInfo[0]);
      SchedulerJobHandle jobId = SchedulerJobHandle.fromString((String) instanceInfo[1]);
      long createdOn = (Long) instanceInfo[2];
      // Get the Runs
      String fetchSQL = "SELECT * FROM " + JOB_INSTANCE_RUN_TABLE + " WHERE " + COLUMN_ID + "=?";
      List<Object[]> instanceRuns = runner.query(fetchSQL, multipleRowsHandler, (String) instanceInfo[0]);
      List<SchedulerJobInstanceRun> runList = processInstanceRun(instanceRuns);
      return new SchedulerJobInstanceInfo(id, jobId, createdOn, runList);
    }

    /**
     * Updates the status of a job instance.
     *
     * @param instanceRun
     * @return number of rows updated.
     * @throws SQLException
     */
    public int updateJobInstanceRun(SchedulerJobInstanceRun instanceRun) throws SQLException {
      String updateSQL =
        "UPDATE " + JOB_INSTANCE_RUN_TABLE + " SET " + COLUMN_END_TIME + "=?, " + COLUMN_RESULT_PATH + "=?, "
          + COLUMN_QUERY_HANDLE + "=?, " + COLUMN_STATUS + "=?" + " WHERE " + COLUMN_ID + "=? AND " + COLUMN_RUN_ID
          + "=?";

      return runner.update(updateSQL, instanceRun.getEndTime(), instanceRun.getResultPath(),
        instanceRun.getQueryHandle() == null ? "" : instanceRun.getQueryHandle().getHandleIdString(),
        instanceRun.getInstanceState().name(), instanceRun.getHandle().getHandleIdString(), instanceRun.getRunId());
    }

    /**
     * Gets all the instance handle of a job
     *
     * @param jobId
     * @return List of SchedulerJobInstanceHandle
     * @throws SQLException
     */
    public List<SchedulerJobInstanceInfo> getAllJobInstances(String jobId) throws SQLException {
      String fetchSQL = "SELECT * FROM " + JOB_INSTANCE_TABLE + " WHERE " + COLUMN_JOB_ID + "=?";
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler, jobId);
      List<SchedulerJobInstanceInfo> resOut = new ArrayList<>();
      for (int i = 0; i < result.size(); i++) {
        Object[] row = result.get(i);
        resOut.add(parseSchedulerInstance(row));
      }
      return resOut;
    }

    /**
     * Get the user who submitted the job.
     *
     * @param id
     * @return
     * @throws SQLException
     */
    public String getUser(String id) throws SQLException {
      String fetchSQL = "SELECT " + COLUMN_USER + " FROM " + JOB_TABLE + " WHERE " + COLUMN_ID + "=?";
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler, id);
      if (result.size() == 0) {
        return null;
      } else {
        return (String) result.get(0)[0];
      }
    }

    private List<SchedulerJobInstanceRun> processInstanceRun(List<Object[]> instanceRuns) throws SQLException {
      List<SchedulerJobInstanceRun> runList = new ArrayList<>();
      for (Object[] run : instanceRuns) {
        // run[0] will contain the instanceID
        SchedulerJobInstanceHandle id = SchedulerJobInstanceHandle.fromString((String) run[0]);
        int runId = (Integer) run[1];
        LensSessionHandle sessionHandle = LensSessionHandle.valueOf((String) run[2]);
        long starttime = (Long) run[3];
        long endtime = (Long) run[4];
        String resultPath = (String) run[5];
        String queryHandleString = (String) run[6];
        QueryHandle queryHandle = null;
        if (!queryHandleString.isEmpty()) {
          queryHandle = QueryHandle.fromString((String) run[6]);
        }
        SchedulerJobInstanceState instanceStatus = SchedulerJobInstanceState.valueOf((String) run[7]);
        SchedulerJobInstanceRun instanceRun = new SchedulerJobInstanceRun(id, runId, sessionHandle, starttime, endtime,
          resultPath, queryHandle, instanceStatus);
        runList.add(instanceRun);
      }
      return runList;
    }

    public List<SchedulerJobInstanceRun> getInstanceRuns(SchedulerJobInstanceState[] states) throws SQLException {
      String whereClause = "";
      for (SchedulerJobInstanceState state : states) {
        whereClause += ((whereClause.isEmpty()) ? " WHERE " : " OR ") + COLUMN_STATUS + " = '" + state + "'";
      }
      String fetchSQL = "SELECT * FROM " + JOB_INSTANCE_RUN_TABLE + whereClause;
      List<Object[]> instanceRuns = runner.query(fetchSQL, multipleRowsHandler);
      return processInstanceRun(instanceRuns);
    }
  }

  /**
   * MySQL based DB Store.
   */
  public static class SchedulerMySQLDBStore extends SchedulerDBStore {
    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobTable() throws SQLException {
      String createSQL =
        "CREATE TABLE IF NOT EXISTS " + JOB_TABLE + "( " + COLUMN_ID + " VARCHAR(255) NOT NULL," + COLUMN_JOB + " TEXT,"
          + COLUMN_USER + " VARCHAR(255)," + COLUMN_STATUS + " VARCHAR(20)," + COLUMN_CREATED_ON + " BIGINT, "
          + COLUMN_MODIFIED_ON + " BIGINT, " + COLUMN_JOB_NAME + " VARCHAR(255), " + " PRIMARY KEY ( " + COLUMN_ID + ")"
          + ")";
      runner.update(createSQL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobInstanceTable() throws SQLException {
      String createSQL =
        "CREATE TABLE IF NOT EXISTS " + JOB_INSTANCE_TABLE + "( " + COLUMN_ID + " VARCHAR(255) NOT NULL, "
          + COLUMN_JOB_ID + " VARCHAR(255) NOT NULL, " + COLUMN_SCHEDULE_TIME + " BIGINT, " + " PRIMARY KEY ( "
          + COLUMN_ID + ")" + ")";
      runner.update(createSQL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobInstanceRunTable() throws SQLException {
      String createSQL =
        "CREATE TABLE IF NOT EXISTS " + JOB_INSTANCE_RUN_TABLE + "( " + COLUMN_ID + " VARCHAR(255) NOT NULL, "
          + COLUMN_RUN_ID + " INT NOT NULL, " + COLUMN_SESSION_HANDLE + " VARCHAR(255), " + COLUMN_START_TIME
          + " BIGINT, " + COLUMN_END_TIME + " BIGINT, " + COLUMN_RESULT_PATH + " TEXT, " + COLUMN_QUERY_HANDLE
          + " VARCHAR(255), " + COLUMN_STATUS + " VARCHAR(20), " + " PRIMARY KEY ( " + COLUMN_ID + ", " + COLUMN_RUN_ID
          + ")" + ")";
      runner.update(createSQL);
    }
  }

  /**
   * HSQL Based DB store. This class is used in testing.
   */
  public static class SchedulerHsqlDBStore extends SchedulerDBStore {
    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobTable() throws SQLException {
      String createSQL =
        "CREATE TABLE IF NOT EXISTS " + JOB_TABLE + "( " + COLUMN_ID + " VARCHAR(255) NOT NULL," + COLUMN_JOB
          + " VARCHAR(1024)," + COLUMN_USER + " VARCHAR(255)," + COLUMN_STATUS + " VARCHAR(20)," + COLUMN_CREATED_ON
          + " BIGINT, " + COLUMN_MODIFIED_ON + " BIGINT, " + COLUMN_JOB_NAME + " VARCHAR(255), " + " PRIMARY KEY ( "
          + COLUMN_ID + ")" + ")";
      runner.update(createSQL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobInstanceTable() throws SQLException {
      String createSQL =
        "CREATE TABLE IF NOT EXISTS " + JOB_INSTANCE_TABLE + "( " + COLUMN_ID + " VARCHAR(255) NOT NULL, "
          + COLUMN_JOB_ID + " VARCHAR(255) NOT NULL, " + COLUMN_SCHEDULE_TIME + " BIGINT, " + " PRIMARY KEY ( "
          + COLUMN_ID + ")" + ")";
      runner.update(createSQL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobInstanceRunTable() throws SQLException {
      String createSQL =
        "CREATE TABLE IF NOT EXISTS " + JOB_INSTANCE_RUN_TABLE + "( " + COLUMN_ID + " VARCHAR(255) NOT NULL, "
          + COLUMN_RUN_ID + " INT NOT NULL, " + COLUMN_SESSION_HANDLE + " VARCHAR(255), " + COLUMN_START_TIME
          + " BIGINT, " + COLUMN_END_TIME + " BIGINT, " + COLUMN_RESULT_PATH + " VARCHAR(1024)," + COLUMN_QUERY_HANDLE
          + " VARCHAR(255), " + COLUMN_STATUS + " VARCHAR(20), " + " PRIMARY KEY ( " + COLUMN_ID + ", " + COLUMN_RUN_ID
          + " )" + ")";
      runner.update(createSQL);
    }
  }
}
