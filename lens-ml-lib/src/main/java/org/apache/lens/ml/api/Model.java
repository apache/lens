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

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The Model class. Contains meta data for a model creation. Algorithm to use, list of features and label. This doesn't
 * contains the actual data for training the model (which is separated and stored in model instance class).
 */
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Model {

  @Getter
  @Setter
  @XmlElement
  private String name;

  @Getter
  @Setter
  private AlgoSpec algoSpec;

  @Getter
  @Setter
  @XmlElement
  private List<Feature> featureSpec;

  @Getter
  @Setter
  private Feature labelSpec;

  /*private static final JAXBContext JAXB_CONTEXT;

  static {
    try {
      JAXB_CONTEXT = JAXBContext.newInstance(Model.class);
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
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
  }*/
}
