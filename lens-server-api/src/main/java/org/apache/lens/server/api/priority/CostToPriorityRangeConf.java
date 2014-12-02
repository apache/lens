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
package org.apache.lens.server.api.priority;


import org.apache.lens.api.Priority;

/**
 * Cost to priority range conf class.
 *
 * @see RangeConf
 */

public class CostToPriorityRangeConf extends RangeConf<Float, Priority> {
  /**
   * Super constructor
   *
   * @param confValue
   * @see RangeConf#RangeConf(String)
   */
  public CostToPriorityRangeConf(String confValue) {
    super(confValue);
  }

  /**
   * Parse key method
   *
   * @param s
   * @return parsed float from string s
   * @see RangeConf#parseKey(String)
   */
  @Override
  protected Float parseKey(String s) {
    return Float.parseFloat(s);
  }

  /**
   * Parse value method
   *
   * @param s
   * @return parsed Priority from String s
   * @see RangeConf#parseValue(String)
   */
  @Override
  protected Priority parseValue(String s) {
    return Priority.valueOf(s);
  }

  /**
   * Default value is "NORMAL". Which implies {@link #get(Comparable)} will always return Priotity.NORMAL
   *
   * @return "NORMAL"
   */
  @Override
  protected String getDefaultConf() {
    return Priority.NORMAL.toString();
  }
}
