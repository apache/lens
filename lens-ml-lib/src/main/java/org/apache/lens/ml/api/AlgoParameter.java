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
package org.apache.lens.ml.api;

import java.io.StringWriter;
import java.lang.annotation.Annotation;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Created by vikassingh on 10/07/15.
 */
@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor
public class AlgoParameter implements AlgoParam {


  private static final JAXBContext JAXB_CONTEXT;

  static {
    try {
      JAXB_CONTEXT = JAXBContext.newInstance(AlgoParameter.class);
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  @Getter
  @Setter
  @XmlElement
  String name;
  @Getter
  @Setter
  @XmlElement
  String help;
  @Getter
  @Setter
  @XmlElement
  String defaultValue;

  @Override
  public String name() {
    return name;
  }

  @Override
  public String help() {
    return help;
  }

  @Override
  public String defaultValue() {
    return defaultValue;
  }

  @Override
  public Class<? extends Annotation> annotationType() {
    return null;
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
