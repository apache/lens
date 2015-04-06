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
package org.apache.lens.ml.server;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

@ApplicationPath("/ml")
public class MLApp extends Application {

  private final Set<Class<?>> classes;

  /**
   * Pass additional classes when running in test mode
   *
   * @param additionalClasses
   */
  public MLApp(Class<?>... additionalClasses) {
    classes = new HashSet<Class<?>>();

    // register root resource
    classes.add(MLServiceResource.class);
    classes.add(MultiPartFeature.class);
    classes.add(LoggingFilter.class);
    for (Class<?> cls : additionalClasses) {
      classes.add(cls);
    }

  }

  /**
   * Get classes for this resource
   */
  @Override
  public Set<Class<?>> getClasses() {
    return classes;
  }
}
