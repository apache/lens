package org.apache.lens.api;

/*
 * #%L
 * Lens API
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.log4j.NDC;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringWriter;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * APIResult is the output returned by all the APIs; status-SUCCEEDED or FAILED
 * message- detailed message.
 */
@XmlRootElement(name = "result")
@XmlAccessorType(XmlAccessType.FIELD)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class APIResult {
  @XmlElement @Getter private Status status;
  @XmlElement @Getter private String message;

  private static final JAXBContext JAXB_CONTEXT;
  static {
    try {
      JAXB_CONTEXT = JAXBContext.newInstance(APIResult.class);
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * API Result status.
   */
  public static enum Status {
    SUCCEEDED, PARTIAL, FAILED
  }

  public APIResult(Status status, String message) {
    super();
    this.status = status;
    this.message = message;
  }

  @Override
  public String toString() {
    try {
      StringWriter stringWriter = new StringWriter();
      Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
      marshaller.marshal(this, stringWriter);
      return stringWriter.toString();
    } catch (JAXBException e) {
      return e.getMessage();
    }
  }
}
