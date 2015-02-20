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
package org.apache.lens.ml;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.ml.MLService;
import org.apache.lens.server.ml.MLServiceImpl;

import org.apache.hadoop.hive.conf.HiveConf;

public final class MLUtils {
  private MLUtils() {
  }

  private static final HiveConf HIVE_CONF;

  static {
    HIVE_CONF = new HiveConf();
    // Add default config so that we know the service provider implementation
    HIVE_CONF.addResource("lensserver-default.xml");
    HIVE_CONF.addResource("lens-site.xml");
  }

  public static String getAlgoName(Class<? extends MLAlgo> algoClass) {
    Algorithm annotation = algoClass.getAnnotation(Algorithm.class);
    if (annotation != null) {
      return annotation.name();
    }
    throw new IllegalArgumentException("Algo should be decorated with annotation - " + Algorithm.class.getName());
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
}
