/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.api.query.save;

import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.Validate;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * The class Parameter.
 *
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@XmlRootElement
public class Parameter {
  /**
   * Name of the parameter used in the query.
   * It should match the following regex pattern ":[a-zA-Z][a-zA-Z_0-9]".
   */
  private String name;

  /**
   * Display name of the parameter. Should be used by the UI apps for resolution.
   */
  private String displayName;

  /**
   * The default value that will be used,
   * if the parameter is not supplied with any values while running the query.
   */
  private String[] defaultValue;

  /**
   * Data type of the parameter. Could be number, decimal, string or boolean.
   * The value supplied will be parsed with the corresponding data type.
   */
  private ParameterDataType dataType;

  /**
   * Collection type of the parameter.
   * Depending on the type of expression IN/EQ, it could be a single/multiple collection.
   */
  private ParameterCollectionType collectionType;

  public Parameter(String name) {
    this.name = name;
  }

  void afterUnmarshal(Unmarshaller u, Object parent) {
    Validate.notNull(name);
    Validate.notNull(displayName);
    Validate.notNull(dataType);
    Validate.notNull(collectionType);
  }
}
