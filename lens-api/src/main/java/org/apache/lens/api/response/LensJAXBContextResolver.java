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
package org.apache.lens.api.response;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.api.error.ErrorCollectionFactory;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

/**
 * @see javax.ws.rs.ext.ContextResolver
 */
@Provider
@Slf4j
public class LensJAXBContextResolver implements ContextResolver<JAXBContext> {

  private Map<Class, JAXBContext> jaxbContextCache = new ConcurrentHashMap<Class, JAXBContext>();

  @Override
  public JAXBContext getContext(Class<?> type) {

    JAXBContext jaxbContext = jaxbContextCache.get(type);

    if (jaxbContext == null) {

      log.debug("JAXB instance to be created for {}", type);
      try {
        if (type.equals(LensResponse.class)) {

          ErrorCollection errorCollection = new ErrorCollectionFactory().createErrorCollection();
          Set<Class> classesToBeBound = Sets.newHashSet(errorCollection.getErrorPayloadClasses());
          log.debug("classesToBeBound:{}", classesToBeBound);
          classesToBeBound.add(type);

          Class[] classesToBeBoundArray = classesToBeBound.toArray(new Class[classesToBeBound.size()]);
          jaxbContext = JAXBContext.newInstance(classesToBeBoundArray);
        } else {

          jaxbContext = JAXBContext.newInstance(type);
        }
        jaxbContextCache.put(type, jaxbContext);

      } catch (JAXBException e) {
        log.error("JAXBContext not initialized for "+type, e);
      } catch (ClassNotFoundException e) {
        log.error("JAXBContext not initialized for "+type, e);
      }
    }
    return jaxbContext;
  }
}
