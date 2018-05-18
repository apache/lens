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
package org.apache.lens.server;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.error.GenericExceptionMapper;
import org.apache.lens.server.error.NotAuthorizedExceptionMapper;

import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class LensApplication.
 */
@ApplicationPath("/")
@Slf4j
public class LensApplication extends Application {

  /** The conf. */
  public static final Configuration CONF = LensServerConf.getHiveConf();

  @Override
  public Set<Class<?>> getClasses() {

    final Set<Class<?>> classes = new HashSet<Class<?>>();

    String[] resourceNames = CONF.getStrings(LensConfConstants.WS_RESOURCE_NAMES);
    String[] featureNames = CONF.getStrings(LensConfConstants.WS_FEATURE_NAMES);
    String[] listenerNames = CONF.getStrings(LensConfConstants.WS_LISTENER_NAMES);
    String[] filterNames = CONF.getStrings(LensConfConstants.WS_FILTER_NAMES);

    // register root resource
    for (String rName : resourceNames) {
      Class wsResourceClass = CONF.getClass(LensConfConstants.getWSResourceImplConfKey(rName), null);
      classes.add(wsResourceClass);
      log.info("Added resource {}", wsResourceClass);
    }
    for (String fName : featureNames) {
      Class wsFeatureClass = CONF.getClass(LensConfConstants.getWSFeatureImplConfKey(fName), null);
      classes.add(wsFeatureClass);
      log.info("Added feature {}", wsFeatureClass);
    }
    for (String lName : listenerNames) {
      Class wsListenerClass = CONF.getClass(LensConfConstants.getWSListenerImplConfKey(lName), null);
      classes.add(wsListenerClass);
      log.info("Added listener {}", wsListenerClass);
    }

    for (String filterName : filterNames) {
      Class wsFilterClass = CONF.getClass(LensConfConstants.getWSFilterImplConfKey(filterName), null);
      classes.add(wsFilterClass);
      log.info("Added filter {}", wsFilterClass);
    }

    classes.add(GenericExceptionMapper.class);
    classes.add(NotAuthorizedExceptionMapper.class);
    return classes;
  }

}
