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
import org.apache.lens.api.scheduler.*;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.scheduler.util.UtilityMethods;

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
      this.store.init(UtilityMethods.getDataSourceFromConf(conf));
      this.store.createJobTable();
      this.store.createJobInstaceTable();
    } catch (SQLException e) {
      log.error("Error creating job tables", e);
      throw new LensException("Error creating job tables ", e);
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
      log.error("Error while storing the jobInfo for " + jobInfo.getId().getHandleIdString(), e);
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
      log.error("Error while getting the job detail for " + id.getHandleIdString(), e);
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
      log.error("Error while getting the job for " + id.getHandleIdString(), e);
      return null;
    }
  }

  /**
   * Gets the Job state
   *
   * @param id : Job handle id.
   * @return SchedulerJobState of the job.
   */
  public SchedulerJobStatus getJobState(SchedulerJobHandle id) {
    try {
      return store.getJobState(id.getHandleIdString());
    } catch (SQLException e) {
      log.error("Error while getting the job state for " + id.getHandleIdString(), e);
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
      log.error("Error while updating job for " + info.getId().getHandleIdString(), e);
      return 0;
    }
  }

  /**
   * Updates the job state form the new SchedulerJobInfo
   *
   * @param info: Updated info objects
   * @return number of rows updated.
   */
  public int updateJobState(SchedulerJobInfo info) {
    try {
      return store.updateJobState(info.getId().getHandleIdString(), info.getState().name(), info.getModifiedOn());
    } catch (SQLException e) {
      log.error("Error while updating job state for " + info.getId().getHandleIdString(), e);
      return 0;
    }
  }

  public int storeJobInstance(SchedulerJobInstanceInfo instanceInfo) {
    try {
      return store.insertIntoJobInstanceTable(instanceInfo);
    } catch (SQLException e) {
      log.error("Error while storing job instance for " + instanceInfo.getId());
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
      log.error("Error while getting the job instance info for " + id.getHandleIdString(), e);
      return null;
    }
  }

  /**
   * Updates the instance state
   *
   * @param info: Updated instance info
   * @return number of rows updated.
   */
  public int updateJobInstanceState(SchedulerJobInstanceInfo info) {
    try {
      return store.updateJobInstanceState(info.getId().getHandleIdString(), info.getState().name());
    } catch (SQLException e) {
      log.error("Error while updating the job instance state for " + info.getId().getHandleIdString(), e);
      return 0;
    }
  }

  /**
   * Gets all the instance handle id for a job.
   *
   * @param id: Job handle id.
   * @return List of instance handles.
   */
  public List<SchedulerJobInstanceHandle> getJobInstances(SchedulerJobHandle id) {
    // TODO: Add number of results to be fetched
    try {
      return store.getAllJobInstances(id.getHandleIdString());
    } catch (SQLException e) {
      log.error("Error while getting instances of a job with id " + id.getHandleIdString(), e);
      return null;
    }
  }

  /**
   * Gets all jobs which match the filter requirements.
   *
   * @param username  : User name of the job
   * @param state     : State of the job
   * @param startTime : Created on should be greater than this start time.
   * @param endTime   : Created on should be less than the end time.
   * @return List of Job handles
   */
  public List<SchedulerJobHandle> getJobs(String username, SchedulerJobStatus state, Long startTime,
      Long endTime) {
    try {
      return store.getJobs(username, state == null ? null : state.name(), startTime, endTime);
    } catch (SQLException e) {
      log.error("Error while getting jobs ", e);
      return null;
    }
  }

  public abstract static class SchedulerDBStore {
    protected static final String JOB_TABLE = "job_table";
    protected static final String JOB_INSTANCE_TABLE = "job_instance_table";
    protected static final String COLUMN_ID = "id";
    protected static final String COLUMN_JOB = "job";
    protected static final String COLUMN_USER = "username";
    protected static final String COLUMN_STATE = "state";
    protected static final String COLUMN_CREATED_ON = "createdon";
    protected static final String COLUMN_MODIFIED_ON = "modifiedon";
    protected static final String COLUMN_JOB_ID = "jobid";
    protected static final String COLUMN_SESSION_HANDLE = "sessionhandle";
    protected static final String COLUMN_START_TIME = "starttime";
    protected static final String COLUMN_END_TIME = "endtime";
    protected static final String COLUMN_RESULT_PATH = "resultpath";
    protected static final String COLUMN_QUERY = "query";
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
    public abstract void createJobInstaceTable() throws SQLException;

    /**
     * Inserts the Job info object into job table
     *
     * @param jobInfo
     * @return number of rows inserted.
     * @throws SQLException
     */
    public int insertIntoJobTable(SchedulerJobInfo jobInfo) throws SQLException {
      String insertSQL = "INSERT INTO " + JOB_TABLE + " VALUES(?,?,?,?,?,?)";
      JAXBElement<XJob> xmlJob = jobFactory.createJob(jobInfo.getJob());
      return runner.update(insertSQL, jobInfo.getId().toString(), ToXMLString.toString(xmlJob), jobInfo.getUserName(),
          jobInfo.getState().name(), jobInfo.getCreatedOn(), jobInfo.getModifiedOn());
    }

    /**
     * Inserts the job instance info object into job instance table
     *
     * @param instanceInfo
     * @return number of rows inserted.
     * @throws SQLException
     */
    public int insertIntoJobInstanceTable(SchedulerJobInstanceInfo instanceInfo) throws SQLException {
      String insertSQL = "INSERT INTO " + JOB_INSTANCE_TABLE + " VALUES(?,?,?,?,?,?,?,?,?)";
      return runner
          .update(insertSQL, instanceInfo.getId().getHandleIdString(), instanceInfo.getJobId().getHandleIdString(),
              instanceInfo.getSessionHandle().toString(), instanceInfo.getStartTime(), instanceInfo.getEndTime(),
              instanceInfo.getResultPath(), instanceInfo.getQuery(), instanceInfo.getState().name(),
              instanceInfo.getCreatedOn());
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
        String state = (String) jobInfo[3];
        long createdOn = (Long) jobInfo[4];
        long modifiedOn = (Long) jobInfo[5];
        return new SchedulerJobInfo(id, xJob, userName, SchedulerJobStatus.valueOf(state), createdOn, modifiedOn);
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
     * Gets the job state
     *
     * @param id
     * @return SchedulerJobState
     * @throws SQLException
     */
    public SchedulerJobStatus getJobState(String id) throws SQLException {
      String fetchSQL = "SELECT " + COLUMN_STATE + " FROM " + JOB_TABLE + " WHERE " + COLUMN_ID + "=?";
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler, id);
      if (result.size() == 0) {
        return null;
      } else {
        return SchedulerJobStatus.valueOf((String) result.get(0)[0]);
      }
    }

    /**
     * Gets all the jobs which match the filter requirements.
     *
     * @param username
     * @param state
     * @param starttime
     * @param endtime
     * @return the list of job handles.
     * @throws SQLException
     */
    public List<SchedulerJobHandle> getJobs(String username, String state, Long starttime, Long endtime)
      throws SQLException {
      String whereClause = "";
      if (username != null && !username.isEmpty()) {
        whereClause += ((whereClause.isEmpty()) ? " WHERE " : " AND ") + COLUMN_USER + " = '" + username+"'";
      }
      if (state != null && !state.isEmpty()) {
        whereClause += ((whereClause.isEmpty()) ? " WHERE " : " AND ") + COLUMN_STATE + " = '" + state + "'";
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
     * Updates the job state into the job table
     *
     * @param id
     * @param state
     * @param modifiedOn
     * @return number of rows updated.
     * @throws SQLException
     */
    public int updateJobState(String id, String state, long modifiedOn) throws SQLException {
      String updateSQL =
          "UPDATE " + JOB_TABLE + " SET " + COLUMN_STATE + "=?, " + COLUMN_MODIFIED_ON + "=? " + " WHERE " + COLUMN_ID
              + "=?";
      return runner.update(updateSQL, state, modifiedOn, id);
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
        SchedulerJobInstanceHandle id = SchedulerJobInstanceHandle.fromString((String) instanceInfo[0]);
        SchedulerJobHandle jobId = SchedulerJobHandle.fromString((String) instanceInfo[1]);
        LensSessionHandle sessionHandle = LensSessionHandle.valueOf((String) instanceInfo[2]);
        long starttime = (Long) instanceInfo[3];
        long endtime = (Long) instanceInfo[4];
        String resultPath = (String) instanceInfo[5];
        String query = (String) instanceInfo[6];
        SchedulerJobInstanceStatus state = SchedulerJobInstanceStatus.valueOf((String) instanceInfo[7]);
        long createdOn = (Long) instanceInfo[8];
        return new SchedulerJobInstanceInfo(id, jobId, sessionHandle, starttime, endtime, resultPath, query, state,
            createdOn);
      }
    }

    /**
     * Updates the state of a job instance.
     *
     * @param id
     * @param state
     * @return number of rows updated.
     * @throws SQLException
     */
    public int updateJobInstanceState(String id, String state) throws SQLException {
      String updateSQL = "UPDATE " + JOB_INSTANCE_TABLE + " SET " + COLUMN_STATE + "=?" + " WHERE " + COLUMN_ID + "=?";
      return runner.update(updateSQL, state, id);
    }

    /**
     * Gets all the instance handle of a job
     *
     * @param jobId
     * @return List of SchedulerJobInstanceHandle
     * @throws SQLException
     */
    public List<SchedulerJobInstanceHandle> getAllJobInstances(String jobId) throws SQLException {
      String fetchSQL = "SELECT " + COLUMN_ID + " FROM " + JOB_INSTANCE_TABLE + " WHERE " + COLUMN_JOB_ID + "=?";
      List<Object[]> result = runner.query(fetchSQL, multipleRowsHandler, jobId);
      List<SchedulerJobInstanceHandle> resOut = new ArrayList<>();
      for (int i = 0; i < result.size(); i++) {
        Object[] row = result.get(i);
        resOut.add(SchedulerJobInstanceHandle.fromString((String) row[0]));
      }
      return resOut;
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
          "CREATE TABLE IF NOT EXISTS " + JOB_TABLE + "( " + COLUMN_ID + " VARCHAR(255) NOT NULL," + COLUMN_JOB
              + " TEXT," + COLUMN_USER + " VARCHAR(255)," + COLUMN_STATE + " VARCHAR(20)," + COLUMN_CREATED_ON
              + " BIGINT, " + COLUMN_MODIFIED_ON + " BIGINT, " + " PRIMARY KEY ( " + COLUMN_ID + ")" + ")";
      runner.update(createSQL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobInstaceTable() throws SQLException {
      String createSQL =
          "CREATE TABLE IF NOT EXISTS " + JOB_INSTANCE_TABLE + "( " + COLUMN_ID + " VARCHAR(255) NOT NULL, "
              + COLUMN_JOB_ID + " VARCHAR(255) NOT NULL, " + COLUMN_SESSION_HANDLE + " VARCHAR(255), "
              + COLUMN_START_TIME + " BIGINT, " + COLUMN_END_TIME + " BIGINT, " + COLUMN_RESULT_PATH + " TEXT, "
              + COLUMN_QUERY + " TEXT, " + COLUMN_STATE + " VARCHAR(20), " + COLUMN_CREATED_ON + " BIGINT, "
              + " PRIMARY KEY ( " + COLUMN_ID + ")" + ")";
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
              + " VARCHAR(1024)," + COLUMN_USER + " VARCHAR(255)," + COLUMN_STATE + " VARCHAR(20)," + COLUMN_CREATED_ON
              + " BIGINT, " + COLUMN_MODIFIED_ON + " BIGINT, " + " PRIMARY KEY ( " + COLUMN_ID + ")" + ")";
      runner.update(createSQL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobInstaceTable() throws SQLException {
      String createSQL =
          "CREATE TABLE IF NOT EXISTS " + JOB_INSTANCE_TABLE + "( " + COLUMN_ID + " VARCHAR(255) NOT NULL, "
              + COLUMN_JOB_ID + " VARCHAR(255) NOT NULL, " + COLUMN_SESSION_HANDLE + " VARCHAR(255), "
              + COLUMN_START_TIME + " BIGINT, " + COLUMN_END_TIME + " BIGINT, " + COLUMN_RESULT_PATH + " VARCHAR(1024),"
              + COLUMN_QUERY + " VARCHAR(1024), " + COLUMN_STATE + " VARCHAR(20), " + COLUMN_CREATED_ON + " BIGINT, "
              + " PRIMARY KEY ( " + COLUMN_ID + ")" + ")";
      runner.update(createSQL);
    }
  }
}
