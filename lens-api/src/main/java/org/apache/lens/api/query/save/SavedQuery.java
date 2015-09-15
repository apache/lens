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

import java.util.List;

import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.Validate;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The class representing the saved query
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@XmlRootElement
public class SavedQuery {
  /**
   * ID of the saved query (unique)
   */
  private long id;

  /**
   * Name of the saved query
   */
  private String name;

  /**
   * Description of the saved query
   */
  private String description;

  /**
   * The actual query. Should adhere to HQL syntax
   */
  private String query;

  /**
   * Parameters that are used in the query
   */
  private List<Parameter> parameters;

  void afterUnmarshal(Unmarshaller u, Object parent) {
    Validate.notNull(name);
    Validate.notNull(query);
    Validate.notNull(parameters);
  }
}
