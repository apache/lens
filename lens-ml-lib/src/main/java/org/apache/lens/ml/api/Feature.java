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

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Feature class. Equivalent of a feature of a Machine Learning model.
 */

@AllArgsConstructor
@XmlRootElement
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class Feature implements Serializable {

  @Getter
  @Setter
  @XmlElement
  private String name;
  @Getter
  @Setter
  @XmlElement
  private String description;
  @Getter
  @Setter
  @XmlElement
  private Type type;
  /**
   * Name of the column of the table to which this feature is mapped.
   */
  @Getter
  @Setter
  @XmlElement
  private String dataColumn;

  @XmlRootElement
  public enum Type {
    Categorical,
    Continuous
  }
}
