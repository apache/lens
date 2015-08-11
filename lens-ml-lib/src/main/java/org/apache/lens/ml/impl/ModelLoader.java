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
package org.apache.lens.ml.impl;

import java.io.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.lens.ml.algo.api.MLModel;
import org.apache.lens.ml.api.MLTestReport;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Load ML models from a FS location.
 */
@Slf4j
public final class ModelLoader {
  private ModelLoader() {
  }

  /** The Constant MODEL_PATH_BASE_DIR. */
  public static final String MODEL_PATH_BASE_DIR = "lens.ml.model.basedir";

  /** The Constant MODEL_PATH_BASE_DIR_DEFAULT. */
  public static final String MODEL_PATH_BASE_DIR_DEFAULT = "file:///tmp";

  /** The Constant TEST_REPORT_BASE_DIR. */
  public static final String TEST_REPORT_BASE_DIR = "lens.ml.test.basedir";

  /** The Constant TEST_REPORT_BASE_DIR_DEFAULT. */
  public static final String TEST_REPORT_BASE_DIR_DEFAULT = MODEL_PATH_BASE_DIR_DEFAULT + "/ml_reports";

  // Model cache settings
  /** The Constant MODEL_CACHE_SIZE. */
  public static final long MODEL_CACHE_SIZE = 10;

  /** The Constant MODEL_CACHE_TIMEOUT. */
  public static final long MODEL_CACHE_TIMEOUT = 3600000L; // one hour

  /** The model cache. */
  private static Cache<Path, MLModel> modelCache = CacheBuilder.newBuilder().maximumSize(MODEL_CACHE_SIZE)
    .expireAfterAccess(MODEL_CACHE_TIMEOUT, TimeUnit.MILLISECONDS).build();

  /**
   * Gets the model location.
   *
   * @param conf      the conf
   * @param algorithm the algorithm
   * @param modelID   the model id
   * @return the model location
   */
  public static Path getModelLocation(Configuration conf, String algorithm, String modelID) {
    String modelDataBaseDir = conf.get(MODEL_PATH_BASE_DIR, MODEL_PATH_BASE_DIR_DEFAULT);
    // Model location format - <modelDataBaseDir>/<algorithm>/modelID
    return new Path(new Path(new Path(modelDataBaseDir), algorithm), modelID);
  }

  /**
   * Load model.
   *
   * @param conf      the conf
   * @param algorithm the algorithm
   * @param modelID   the model id
   * @return the ML model
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static MLModel loadModel(Configuration conf, String algorithm, String modelID) throws IOException {
    final Path modelPath = getModelLocation(conf, algorithm, modelID);
    log.info("Loading model for algorithm: {} modelID: {} At path: {}", algorithm, modelID,
      modelPath.toUri().toString());
    try {
      return modelCache.get(modelPath, new Callable<MLModel>() {
        @Override
        public MLModel call() throws Exception {
          FileSystem fs = modelPath.getFileSystem(new HiveConf());
          if (!fs.exists(modelPath)) {
            throw new IOException("Model path not found " + modelPath.toString());
          }

          ObjectInputStream ois = null;
          try {
            ois = new ObjectInputStream(fs.open(modelPath));
            MLModel model = (MLModel) ois.readObject();
            log.info("Loaded model {} from location {}", model.getId(), modelPath);
            return model;
          } catch (ClassNotFoundException e) {
            throw new IOException(e);
          } finally {
            IOUtils.closeQuietly(ois);
          }
        }
      });
    } catch (ExecutionException exc) {
      throw new IOException(exc);
    }
  }

  /**
   * Clear cache.
   */
  public static void clearCache() {
    modelCache.cleanUp();
  }

  /**
   * Gets the test report path.
   *
   * @param conf      the conf
   * @param algorithm the algorithm
   * @param report    the report
   * @return the test report path
   */
  public static Path getTestReportPath(Configuration conf, String algorithm, String report) {
    String testReportDir = conf.get(TEST_REPORT_BASE_DIR, TEST_REPORT_BASE_DIR_DEFAULT);
    return new Path(new Path(testReportDir, algorithm), report);
  }

  /**
   * Save test report.
   *
   * @param conf   the conf
   * @param report the report
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void saveTestReport(Configuration conf, MLTestReport report) throws IOException {
    Path reportDir = new Path(conf.get(TEST_REPORT_BASE_DIR, TEST_REPORT_BASE_DIR_DEFAULT));
    FileSystem fs = reportDir.getFileSystem(conf);

    if (!fs.exists(reportDir)) {
      log.info("Creating test report dir {}", reportDir.toUri().toString());
      fs.mkdirs(reportDir);
    }

    Path algoDir = new Path(reportDir, report.getAlgorithm());

    if (!fs.exists(algoDir)) {
      log.info("Creating algorithm report dir {}", algoDir.toUri().toString());
      fs.mkdirs(algoDir);
    }

    ObjectOutputStream reportOutputStream = null;
    Path reportSaveLocation;
    try {
      reportSaveLocation = new Path(algoDir, report.getReportID());
      reportOutputStream = new ObjectOutputStream(fs.create(reportSaveLocation));
      reportOutputStream.writeObject(report);
      reportOutputStream.flush();
    } catch (IOException ioexc) {
      log.error("Error saving test report {}", report.getReportID(), ioexc);
      throw ioexc;
    } finally {
      IOUtils.closeQuietly(reportOutputStream);
    }
    log.info("Saved report {} at location {}", report.getReportID(), reportSaveLocation.toUri());
  }

  /**
   * Load report.
   *
   * @param conf      the conf
   * @param algorithm the algorithm
   * @param reportID  the report id
   * @return the ML test report
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static MLTestReport loadReport(Configuration conf, String algorithm, String reportID) throws IOException {
    Path reportLocation = getTestReportPath(conf, algorithm, reportID);
    FileSystem fs = reportLocation.getFileSystem(conf);
    ObjectInputStream reportStream = null;
    MLTestReport report = null;

    try {
      reportStream = new ObjectInputStream(fs.open(reportLocation));
      report = (MLTestReport) reportStream.readObject();
    } catch (IOException ioex) {
      log.error("Error reading report {}", reportLocation, ioex);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(reportStream);
    }
    return report;
  }

  /**
   * Delete model.
   *
   * @param conf      the conf
   * @param algorithm the algorithm
   * @param modelID   the model id
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void deleteModel(HiveConf conf, String algorithm, String modelID) throws IOException {
    Path modelLocation = getModelLocation(conf, algorithm, modelID);
    FileSystem fs = modelLocation.getFileSystem(conf);
    fs.delete(modelLocation, false);
  }

  /**
   * Delete test report.
   *
   * @param conf      the conf
   * @param algorithm the algorithm
   * @param reportID  the report id
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void deleteTestReport(HiveConf conf, String algorithm, String reportID) throws IOException {
    Path reportPath = getTestReportPath(conf, algorithm, reportID);
    reportPath.getFileSystem(conf).delete(reportPath, false);
  }
}
