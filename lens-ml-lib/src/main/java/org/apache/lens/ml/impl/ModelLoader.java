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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.lens.ml.algo.api.TrainedModel;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ModelLoader {
  /**
   * The Constant MODEL_PATH_BASE_DIR.
   */
  public static final String MODEL_PATH_BASE_DIR = "lens.ml.model.basedir";

  /**
   * The Constant MODEL_PATH_BASE_DIR_DEFAULT.
   */
  public static final String MODEL_PATH_BASE_DIR_DEFAULT = "file:///tmp";
  /**
   * The Constant LOG.
   */
  public static final Log LOG = LogFactory.getLog(ModelLoader.class);
  /**
   * The Constant TEST_REPORT_BASE_DIR.
   */
  public static final String TEST_REPORT_BASE_DIR = "lens.ml.test.basedir";
  /**
   * The Constant TEST_REPORT_BASE_DIR_DEFAULT.
   */
  public static final String TEST_REPORT_BASE_DIR_DEFAULT = "file:///tmp/ml_reports";
  /**
   * The Constant MODEL_CACHE_SIZE.
   */
  public static final long MODEL_CACHE_SIZE = 10;

  // Model cache settings
  /**
   * The Constant MODEL_CACHE_TIMEOUT.
   */
  public static final long MODEL_CACHE_TIMEOUT = 3600000L; // one hour
  /**
   * The model cache.
   */
  private static Cache<Path, TrainedModel> modelCache = CacheBuilder.newBuilder().maximumSize(MODEL_CACHE_SIZE)
    .expireAfterAccess(MODEL_CACHE_TIMEOUT, TimeUnit.MILLISECONDS).build();

  private ModelLoader() {
  }

  /**
   * Gets the model location.
   *
   * @param conf      the conf
   * @param algorithm the algorithm
   * @param modelID   the model id
   * @return the model location
   */
  public static Path getModelLocation(Configuration conf, String algorithm, String modelID, String modelInstanceId) {
    String modelDataBaseDir = conf.get(MODEL_PATH_BASE_DIR, MODEL_PATH_BASE_DIR_DEFAULT);
    // Model location format - <modelDataBaseDir>/<algorithm>/modelID
    return new Path(new Path(new Path(new Path(modelDataBaseDir), algorithm), modelID), modelInstanceId);
  }

  /**
   * @param conf
   * @param algorithm
   * @param modelID
   * @return
   * @throws IOException
   */
  public static TrainedModel loadModel(Configuration conf, String algorithm, final String modelID,
                                       String modelInstanceId) throws IOException {
    final Path modelPath = getModelLocation(conf, algorithm, modelID, modelInstanceId);
    LOG.info("Loading model for algorithm: " + algorithm + " modelID: " + modelID + " At path: "
      + modelPath.toUri().toString());
    try {
      return modelCache.get(modelPath, new Callable<TrainedModel>() {
        @Override
        public TrainedModel call() throws Exception {
          FileSystem fs = modelPath.getFileSystem(new HiveConf());
          if (!fs.exists(modelPath)) {
            throw new IOException("Model path not found " + modelPath.toString());
          }

          ObjectInputStream ois = null;
          try {
            ois = new ObjectInputStream(fs.open(modelPath));
            TrainedModel model = (TrainedModel) ois.readObject();
            LOG.info("Loaded model " + modelID + " from location " + modelPath);
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
   * Delete model.
   *
   * @param conf      the conf
   * @param algorithm the algorithm
   * @param modelID   the model id
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void deleteModel(HiveConf conf, String algorithm, String modelID, String modelInstanceId)
    throws IOException {
    Path modelLocation = getModelLocation(conf, algorithm, modelID, modelInstanceId);
    FileSystem fs = modelLocation.getFileSystem(conf);
    fs.delete(modelLocation, false);
  }
}
