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
package org.apache.lens.api;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.UUID;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * The Class LensSessionHandle.
 */
@XmlRootElement
/**
 * Instantiates a new lens session handle.
 *
 * @param publicId
 *          the public id
 * @param secretId
 *          the secret id
 */
@AllArgsConstructor
/**
 * Instantiates a new lens session handle.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LensSessionHandle {

  /** The public id. */
  @XmlElement
  @Getter
  private UUID publicId;

  /** The secret id. */
  @XmlElement
  @Getter
  private UUID secretId;

  /** The Constant JAXB_CONTEXT. */
  private static final JAXBContext JAXB_CONTEXT;
  static {
    try {
      JAXB_CONTEXT = JAXBContext.newInstance(LensSessionHandle.class);
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Value of.
   *
   * @param sessionStr
   *          the session str
   * @return the lens session handle
   */
  public static LensSessionHandle valueOf(String sessionStr) {
    try {
      Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
      return (LensSessionHandle) unmarshaller.unmarshal(new StringReader(sessionStr));
    } catch (JAXBException e) {
      return null;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    try {
      StringWriter stringWriter = new StringWriter();
      Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
      marshaller.marshal(this, stringWriter);
      return stringWriter.toString();
    } catch (JAXBException e) {
      return "";
    }
  }
}
