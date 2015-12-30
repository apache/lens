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
/*
 *
 */
package org.apache.lens.api.jaxb;

import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.bind.*;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.util.ValidationEventCollector;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LensJAXBContext extends JAXBContext {
  private final JAXBContext jaxbContext;
  private final boolean hasTopLevelClass;

  public LensJAXBContext(Class... classesToBeBoundArray) throws JAXBException {
    jaxbContext = JAXBContext.newInstance(classesToBeBoundArray);
    boolean hasTopLevelClass = false;
    for (Class clas : classesToBeBoundArray) {
      if (clas.isAnnotationPresent(XmlRootElement.class)) {
        hasTopLevelClass = true;
        break;
      }
    }
    this.hasTopLevelClass = hasTopLevelClass;
  }

  @Override
  public Unmarshaller createUnmarshaller() throws JAXBException {
    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    if (!hasTopLevelClass) {
      ClassLoader classLoader = LensJAXBContext.class.getClassLoader();
      SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      try {
        URL resource = classLoader.getResource("cube-0.1.xsd");
        Schema schema = sf.newSchema(resource);
        unmarshaller.setSchema(schema);
      } catch (SAXException e) {
        throw new JAXBException(e);
      }
    }
    unmarshaller.setEventHandler(new LensValidationEventCollector());
    return unmarshaller;
  }

  @Override
  public Marshaller createMarshaller() throws JAXBException {
    return jaxbContext.createMarshaller();
  }

  @Override
  public Validator createValidator() throws JAXBException {
    return jaxbContext.createValidator();
  }

  private static class LensValidationEventCollector extends ValidationEventCollector {
    @Override
    public boolean handleEvent(ValidationEvent event) {
      if (event.getSeverity() == event.ERROR || event.getSeverity() == event.FATAL_ERROR) {
        throw new LensJAXBValidationException(event);
      }
      return true;
    }
  }
}
