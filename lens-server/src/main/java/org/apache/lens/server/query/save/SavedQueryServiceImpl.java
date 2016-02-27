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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query.save;

import static org.apache.lens.api.error.LensCommonErrorCode.INVALID_XML_ERROR;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.save.ListResponse;
import org.apache.lens.api.query.save.SavedQuery;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.LensErrorInfo;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.query.save.SavedQueryHelper;
import org.apache.lens.server.api.query.save.SavedQueryService;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;

import lombok.NonNull;

public class SavedQueryServiceImpl extends BaseLensService implements SavedQueryService {
  private SavedQueryDao dao;
  private Configuration conf;

  public static final String NAME = "savedquery";

  /**
   * Instantiates a new lens service.
   *
   * @param cliService the cli service
   */
  public SavedQueryServiceImpl(CLIService cliService) throws LensException {
    super(NAME, cliService);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    conf = hiveConf;
    @NonNull final String dialect = conf.get(LensConfConstants.JDBC_DIALECT_PROVIDER_CLASS_KEY
      , SavedQueryDao.HSQLDialect.class.getCanonicalName());
    try {
      dao = new SavedQueryDao(
        dialect,
        new QueryRunner(UtilityMethods.getPoolingDataSourceFromConf(conf))
      );
    } catch (LensException e) {
      throw new RuntimeException("Cannot initialize saved query service", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HealthStatus getHealthStatus() {
    return this.getServiceState().equals(STATE.STARTED)
      ? new HealthStatus(true, "Saved query service is healthy.")
      : new HealthStatus(false, "Saved query service is down.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long save(LensSessionHandle handle, SavedQuery savedQuery) throws LensException {
    try {
      acquire(handle);
      validateSampleResolved(savedQuery);
      return dao.saveQuery(savedQuery);
    } finally {
      release(handle);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void update(LensSessionHandle handle, long id, SavedQuery savedQuery) throws LensException {
    try {
      acquire(handle);
      validateSampleResolved(savedQuery);
      dao.updateQuery(id, savedQuery);
    } finally {
      release(handle);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(LensSessionHandle handle, long id) throws LensException {
    try {
      acquire(handle);
      dao.deleteSavedQueryByID(id);
    } finally {
      release(handle);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SavedQuery get(LensSessionHandle handle, long id) throws LensException {
    try {
      acquire(handle);
      return dao.getSavedQueryByID(id);
    } finally {
      release(handle);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListResponse list(LensSessionHandle handle,
    MultivaluedMap<String, String> criteria, long start, long count) throws LensException {
    try {
      acquire(handle);
      return dao.getList(criteria, start, count);
    } finally {
      release(handle);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void grant(LensSessionHandle handle, long id, String sharingUser, String targetUserPath, String[] privileges)
    throws LensException {
    //NOOP
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void revoke(LensSessionHandle handle, long id, String sharingUser, String targetUserPath, String[] privileges)
    throws LensException {
    //NOOP
  }
  /**
   * Validates the saved query and throws LensException with.
   * BAD_SYNTAX code if wrong
   *
   * @param savedQuery Saved query object
   * @throws LensException if invalid
   */
  private void validateSampleResolved(@NonNull SavedQuery savedQuery) throws LensException {
    final String sampleResolved  = SavedQueryHelper.getSampleResolvedQuery(savedQuery);
    try {
      HQLParser.parseHQL(sampleResolved, new HiveConf());
    } catch (Exception e) {
      throw new LensException(
        new LensErrorInfo(INVALID_XML_ERROR.getValue(), 0, INVALID_XML_ERROR.toString())
        , e
        , "Encountered while resolving with sample values { " + sampleResolved + " }");
    }
  }

}
