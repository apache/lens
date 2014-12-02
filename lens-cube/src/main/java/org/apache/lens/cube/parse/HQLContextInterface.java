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
package org.apache.lens.cube.parse;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * HQL context holding the ql expressions
 */
public interface HQLContextInterface {

  /**
   * Get the HQL query.
   * 
   * @return query string
   * @throws SemanticException
   */
  public String toHQL() throws HiveException;

  /**
   * Get select expression.
   * 
   * @return select
   */
  public String getSelect();

  /**
   * Get from string
   * 
   * @return from
   */
  public String getFrom();

  /**
   * Get where string
   * 
   * @return where
   */
  public String getWhere();

  /**
   * Get groupby string
   * 
   * @return groupby
   */
  public String getGroupby();

  /**
   * Get having string
   * 
   * @return having
   */
  public String getHaving();

  /**
   * Get orderby string
   * 
   * @return orderby
   */
  public String getOrderby();

  /**
   * Get limit
   * 
   * @return limit
   */
  public Integer getLimit();
}
