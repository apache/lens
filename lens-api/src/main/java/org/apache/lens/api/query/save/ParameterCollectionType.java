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
package org.apache.lens.api.query.save;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * The enum ParameterCollectionType
 * Collection type of a parameter has to be chosen based on its context.
 * - If it is occurring next to an IN/NOT IN clause, its multiple
 * - If it is found with EQ/NEQ..&gt;,&lt;,&gt;=,&lt;=,like etc, its single
 */
@XmlRootElement
public enum ParameterCollectionType {
  /**
   * Single valued parameter.
   */
  SINGLE,

  /**
   * Multivalued parameter.
   */
  MULTIPLE;
}
