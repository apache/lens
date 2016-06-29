/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.api;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.*;

import org.apache.lens.api.jaxb.LensJAXBContext;

public abstract class ToXMLString {
  protected static final Map<Class<?>, JAXBContext> JAXB_CONTEXTS = new HashMap<>();

  public static String toString(Object o) {
    try {
      StringWriter stringWriter = new StringWriter();
      Class cl = null;
      if (o instanceof JAXBElement) {
        cl = ((JAXBElement) o).getDeclaredType();
      } else {
        cl = o.getClass();
      }
      Marshaller marshaller = getLensJAXBContext(cl).createMarshaller();
      marshaller.marshal(o, stringWriter);
      return stringWriter.toString();
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  public static JAXBContext getLensJAXBContext(Class<?> clazz) {
    if (!JAXB_CONTEXTS.containsKey(clazz)) {
      try {
        JAXB_CONTEXTS.put(clazz, new LensJAXBContext(clazz));
      } catch (JAXBException e) {
        throw new RuntimeException(e);
      }
    }
    return JAXB_CONTEXTS.get(clazz);
  }

  public static <T> T valueOf(String sessionStr, Class tClass) {
    try {
      Unmarshaller unmarshaller = getLensJAXBContext(tClass).createUnmarshaller();
      Object ret = unmarshaller.unmarshal(new StringReader(sessionStr));
      if (ret instanceof JAXBElement) {
        return ((JAXBElement<T>) ret).getValue();
      }
      return (T) ret;
    } catch (JAXBException e) {
      return null;
    }
  }

  @Override
  public String toString() {
    return toString(this);
  }
}
