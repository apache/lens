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
import java.io.ObjectOutputStream;

import org.apache.lens.ml.algo.api.TrainedModel;
import org.apache.lens.ml.api.MLConfConstants;
import org.apache.lens.ml.api.Model;
import org.apache.lens.ml.server.MLService;
import org.apache.lens.ml.server.MLServiceImpl;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import org.datanucleus.store.rdbms.datasource.dbcp.BasicDataSource;

public final class MLUtils {
  private static final HiveConf HIVE_CONF;

  private static final Log LOG = LogFactory.getLog(MLUtils.class);

  static {
    HIVE_CONF = new HiveConf();
    // Add default config so that we know the service provider implementation
    HIVE_CONF.addResource("lensserver-default.xml");
    HIVE_CONF.addResource("lens-site.xml");
  }

  private MLUtils() {
  }

  public static MLServiceImpl getMLService() throws Exception {
    return getServiceProvider().getService(MLService.NAME);
  }

  public static ServiceProvider getServiceProvider() throws Exception {
    Class<? extends ServiceProviderFactory> spfClass = HIVE_CONF.getClass(LensConfConstants.SERVICE_PROVIDER_FACTORY,
      null, ServiceProviderFactory.class);
    ServiceProviderFactory spf = spfClass.newInstance();
    return spf.getServiceProvider();
  }

  public static Path persistModel(TrainedModel trainedModel, Model model, String modelInstanceId) throws IOException {
    Path algoDir = getAlgoDir(model.getAlgoSpec().getAlgo());
    FileSystem fs = algoDir.getFileSystem(HIVE_CONF);

    if (!fs.exists(algoDir)) {
      fs.mkdirs(algoDir);
    }

    Path modelSavePath = new Path(algoDir, new Path(model.getName(), modelInstanceId));
    ObjectOutputStream outputStream = null;

    try {
      outputStream = new ObjectOutputStream(fs.create(modelSavePath, false));
      outputStream.writeObject(trainedModel);
      outputStream.flush();
    } catch (IOException io) {
      LOG.error("Error saving model " + modelInstanceId + " reason: " + io.getMessage());
      throw io;
    } finally {
      IOUtils.closeQuietly(outputStream);
    }
    return modelSavePath;
  }

  public static Path getAlgoDir(String algoName) throws IOException {
    String modelSaveBaseDir = HIVE_CONF.get(ModelLoader.MODEL_PATH_BASE_DIR, ModelLoader.MODEL_PATH_BASE_DIR_DEFAULT);
    return new Path(new Path(modelSaveBaseDir), algoName);
  }

  public static BasicDataSource createMLMetastoreConnectionPool(Configuration conf) {
    BasicDataSource tmp = new BasicDataSource();
    tmp.setDriverClassName(conf.get(MLConfConstants.ML_META_STORE_DB_DRIVER_NAME,
      MLConfConstants.DEFAULT_ML_META_STORE_DB_DRIVER_NAME));
    tmp.setUrl(conf.get(MLConfConstants.ML_META_STORE_DB_JDBC_URL, MLConfConstants.DEFAULT_ML_META_STORE_DB_JDBC_URL));
    tmp
      .setUsername(conf.get(MLConfConstants.ML_META_STORE_DB_JDBC_USER, MLConfConstants.DEFAULT_ML_META_STORE_DB_USER));
    tmp
      .setPassword(conf.get(MLConfConstants.ML_META_STORE_DB_JDBC_PASS, MLConfConstants.DEFAULT_ML_META_STORE_DB_PASS));
    //tmp.setValidationQuery(conf.get(MLConfConstants.ML_META_STORE_DB_VALIDATION_QUERY,
    //  MLConfConstants.DEFAULT_ML_META_STORE_DB_VALIDATION_QUERY));
    tmp.setInitialSize(conf.getInt(MLConfConstants.ML_META_STORE_DB_SIZE, MLConfConstants
      .DEFAULT_ML_META_STORE_DB_SIZE));
    tmp.setDefaultAutoCommit(true);
    return tmp;
  }
}
