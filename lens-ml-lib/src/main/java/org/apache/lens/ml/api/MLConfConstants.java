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
package org.apache.lens.ml.api;

import org.apache.lens.server.api.error.LensException;

/**
 * The class MLConfConstants.
 */
public class MLConfConstants {

  public static final String SERVER_PFX = "lens.server.ml.";
  /**
   * Minimum thread pool size for worked threads who performing ML process i.e. running MLEvaluation,
   * prediction or evaluation
   */
  public static final int DEFAULT_EXECUTOR_POOL_MIN_THREADS = 3;
  public static final String EXECUTOR_POOL_MIN_THREADS = SERVER_PFX + "executor.pool.min.threads";
  /**
   * Maximum thread pool size for worked threads who performing ML process i.e. running MLEvaluation,
   * prediction or evaluation
   */
  public static final int DEFAULT_EXECUTOR_POOL_MAX_THREADS = 100;
  public static final String EXECUTOR_POOL_MAX_THREADS = SERVER_PFX + "executor.pool.max.threads";
  /**
   * keep alive time for threads in the MLProcess thread pool
   */
  public static final int DEFAULT_CREATOR_POOL_KEEP_ALIVE_MILLIS = 60000;

  /**
   * Minimum thread pool size for worked threads who performing ML process i.e. running MLEvaluation,
   * prediction or evaluation
   */
  /**
   * This is the time a MLprocess will be in cache after it is finished. After which request for that MLprocess will
   * be server from the Meta store.
   */
  public static final long DEFAULT_ML_PROCESS_CACHE_LIFE = 1000 * 60 * 10; //10 min.
  public static final String ML_PROCESS_CACHE_LIFE = SERVER_PFX + "mlprocess.cache.life";
  /**
   * This is the maximum time allowed for a MLProcess to run. After which it will be killed by the MLProcesPurger
   * thread.
   */
  public static final long DEFAULT_ML_PROCESS_MAX_LIFE = 1000 * 60 * 60 * 10; // 10 hours.
  public static final String ML_PROCESS_MAX_LIFE = SERVER_PFX + "mlprocess.max.life";
  /**
   * The Constant UDF_NAME.
   */
  public static final String UDF_NAME = "predict";
  /**
   * prefix for output table which gets created for any prediction.
   */
  public static final String PREDICTION_OUTPUT_TABLE_PREFIX = "prediction_";
  /**
   * prefix for output table which gets created for any evaluation.
   */
  public static final String EVALUATION_OUTPUT_TABLE_PREFIX = "evaluation_";
  public static final String ML_META_STORE_DB_DRIVER_NAME = "";
  public static final String DEFAULT_ML_META_STORE_DB_DRIVER_NAME = "com.mysql.jdbc.Driver";
  public static final String ML_META_STORE_DB_JDBC_URL = "";
  public static final String DEFAULT_ML_META_STORE_DB_JDBC_URL = "jdbc:mysql://localhost/lens_ml";
  public static final String ML_META_STORE_DB_JDBC_USER = "";
  public static final String DEFAULT_ML_META_STORE_DB_USER = "root";
  public static final String ML_META_STORE_DB_JDBC_PASS = "";
  public static final String DEFAULT_ML_META_STORE_DB_PASS = "";
  public static final String ML_META_STORE_DB_VALIDATION_QUERY = "";
  public static final String DEFAULT_ML_META_STORE_DB_VALIDATION_QUERY = "select 1 from datasets";
  public static final String ML_META_STORE_DB_SIZE = "";
  public static final int DEFAULT_ML_META_STORE_DB_SIZE = 10;

  private MLConfConstants() throws LensException {
    throw new LensException("Can't instantiate MLConfConstants");
  }
}
