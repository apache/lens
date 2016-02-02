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

import javax.ws.rs.core.Application;

import org.apache.lens.api.jaxb.LensJAXBContextResolver;
import org.apache.lens.api.util.MoxyJsonConfigurationContextResolver;
import org.apache.lens.server.error.LensJAXBValidationExceptionMapper;

import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;

public abstract class BaseApp extends Application {

  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<>();
    // register root resource
    classes.add(getResource());
    classes.add(MultiPartFeature.class);
    classes.add(LensJAXBContextResolver.class);
    classes.add(LensJAXBValidationExceptionMapper.class);
    classes.add(LensRequestContextInitFilter.class);
    classes.add(LoggingFilter.class);
    classes.add(LensApplicationListener.class);
    classes.add(MoxyJsonConfigurationContextResolver.class);
    classes.add(MoxyJsonFeature.class);
    return classes;
  }

  protected abstract Class getResource();
}
