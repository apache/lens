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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.XMLConstants;
import javax.xml.bind.*;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.util.ValidationEventCollector;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LensJAXBContext extends JAXBContext {
  private final JAXBContext jaxbContext;
  private final boolean hasTopLevelClass;
  private static final LensJAXBContext INSTANCE;
  private static final Unmarshaller UNMARSHALLER;

  static {
    try {
      INSTANCE = new LensJAXBContext(org.apache.lens.api.metastore.ObjectFactory.class,
        org.apache.lens.api.scheduler.ObjectFactory.class);
      UNMARSHALLER = INSTANCE.createUnmarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException("Couldn't create instance of lens jaxb context", e);
    }
  }

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
      SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      try {
        Schema schema = sf.newSchema(new Source[]{
          new StreamSource(getClass().getResourceAsStream("/scheduler-job-0.1.xsd")),
          new StreamSource(getClass().getResourceAsStream("/cube-0.1.xsd")),
        });
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

  public LensJAXBContext getInstance() {
    return INSTANCE;
  }

  public Unmarshaller getUnmarshaller() {
    return UNMARSHALLER;
  }

  public static <T> T unmarshallFromFile(String filename) throws JAXBException, IOException {
    File file = new File(filename);
    if (file.exists()) {
      return ((JAXBElement<T>) UNMARSHALLER.unmarshal(file)).getValue();
    } else {
      // load from classpath
      InputStream stream = LensJAXBContext.class.getResourceAsStream(filename);
      if (stream == null) {
        throw new IOException("File not found:" + filename);
      }
      return ((JAXBElement<T>) UNMARSHALLER.unmarshal(stream)).getValue();
    }
  }
}
