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

import java.util.Map;

import javax.xml.bind.annotation.*;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The Algo Spec class. This class works as a particular instance of an Algorithm run. since it contains the exact
 * algoParams with which an Algorithm was run.
 */
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AlgoSpec {
  @Getter
  @Setter
  @XmlElement
  private String algo;
  @Getter
  @Setter
  @XmlElementWrapper
  private Map<String, String> algoParams;

  /*private static final JAXBContext JAXB_CONTEXT;

  /*static {
    try {
      JAXB_CONTEXT = JAXBContext.newInstance(AlgoSpec.class);
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
