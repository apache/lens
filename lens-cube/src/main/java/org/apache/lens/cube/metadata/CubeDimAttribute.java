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
package org.apache.lens.cube.metadata;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class CubeDimAttribute extends CubeColumn {

  public CubeDimAttribute(String name, String description) {
    this(name, description, null, null, null, null, null);
  }

  public CubeDimAttribute(String name, String description, String displayString, Date startTime, Date endTime,
                          Double cost) {
    this(name, description, displayString, startTime, endTime, cost, new HashMap<String, String>());
  }

  public CubeDimAttribute(String name, String description, String displayString, Date startTime, Date endTime,
    Double cost, Map<String, String> tags) {
    super(name, description, displayString, startTime, endTime, cost, tags);
  }

  public CubeDimAttribute(String name, Map<String, String> props) {
    super(name, props);
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getDimensionClassPropertyKey(getName()), getClass().getName());
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

}
