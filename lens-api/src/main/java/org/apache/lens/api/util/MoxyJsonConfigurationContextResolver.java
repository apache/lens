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
package org.apache.lens.api.util;

import javax.ws.rs.ext.ContextResolver;

import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.glassfish.jersey.moxy.json.MoxyJsonConfig;

public final class MoxyJsonConfigurationContextResolver implements ContextResolver<MoxyJsonConfig> {

  @Override
  public MoxyJsonConfig getContext(final Class<?> type) {
    final MoxyJsonConfig configuration = new MoxyJsonConfig();
    configuration.setIncludeRoot(true);
    configuration.setFormattedOutput(true);
    configuration.setMarshalEmptyCollections(false);
    configuration.marshallerProperty(MarshallerProperties.JSON_WRAPPER_AS_ARRAY_NAME, true);
    configuration.unmarshallerProperty(MarshallerProperties.JSON_WRAPPER_AS_ARRAY_NAME, true);
    return configuration;
  }
}
