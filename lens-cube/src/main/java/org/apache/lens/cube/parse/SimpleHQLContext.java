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

import org.apache.lens.server.api.error.LensException;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Accepts strings of all expressions and constructs HQL query.
 * <p></p>
 * Making this as an abstract class because it provides constructors without all expressions being set.
 */
@Slf4j
@Data
public abstract class SimpleHQLContext implements HQLContextInterface {

  private String select;
  protected String from;
  private String where;
  private String groupby;
  private String orderby;
  private String having;
  private Integer limit;


  protected void setQueryParts(QueryAST ast) {
    select = ast.getSelectString(); groupby = ast.getGroupByString(); orderby= ast.getOrderByString();
      having=ast.getHavingString(); limit=ast.getLimitValue();
  }

  /**
   * Set all missing expressions of HQL context.
   * <p></p>
   * Leaving this empty implementation for the case of all expressions being passed in constructor. If other
   * constructors are used the missing expressions should be set here
   *
   * @throws LensException
   */
  protected void setMissingExpressions() throws LensException {
  }

  public String toHQL() throws LensException {
    setMissingExpressions();
    return CandidateUtil.buildHQLString(select, from, where, groupby, orderby, having, limit);
  }
}
