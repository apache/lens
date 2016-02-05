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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * The Class LensConf.
 */
@XmlRootElement(name = "conf")
/**
 * Instantiates a new lens conf.
 */
@NoArgsConstructor
public class LensConf extends ToYAMLString implements Serializable {

  /**
   * The Constant serialVersionUID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * The properties.
   */
  @XmlElementWrapper
  @Getter
  private final Map<String, String> properties = new HashMap<>();

  /**
   * Adds the property.
   *
   * @param key   the key
   * @param value the value
   */
  public void addProperty(String key, String value) {
    properties.put(key, value);
  }

  public void addProperty(Object key, Object value) {
    properties.put(String.valueOf(key), String.valueOf(value));
  }

  /**
   * Adds Map of properties.
   *
   * @param props the properties
   */
  public void addProperties(Map<String, String> props) {
    properties.putAll(props);
  }

  public String getProperty(Object key) {
    return properties.get(key);
  }
}
