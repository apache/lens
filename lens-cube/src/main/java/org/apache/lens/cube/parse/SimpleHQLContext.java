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

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
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
  private String prefix;
  private String select;
  private String from;
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
  protected abstract void setMissingExpressions() throws LensException;

  public String toHQL() throws LensException {
    setMissingExpressions();
    return buildHQLString();
  }

  private static final String BASE_QUERY_FORMAT = "SELECT %s FROM %s";

  public String buildHQLString() {
    return buildHQLString(prefix, select, from, where, groupby, orderby, having, limit);
  }
  public static String buildHQLString(String prefix, String select, String from, String where,
    String groupby, String orderby, String having, Integer limit) {
    StringBuilder queryFormat = new StringBuilder();
    List<String> qstrs = Lists.newArrayList();
    if (!StringUtils.isBlank(prefix)) {
      queryFormat.append("%s");
      qstrs.add(prefix);
    }
    queryFormat.append(BASE_QUERY_FORMAT);
    qstrs.add(select);
    qstrs.add(from);
    if (!StringUtils.isBlank(where)) {
      queryFormat.append(" WHERE %s");
      qstrs.add(where);
    }
    if (!StringUtils.isBlank(groupby)) {
      queryFormat.append(" GROUP BY %s");
      qstrs.add(groupby);
    }
    if (!StringUtils.isBlank(having)) {
      queryFormat.append(" HAVING %s");
      qstrs.add(having);
    }
    if (!StringUtils.isBlank(orderby)) {
      queryFormat.append(" ORDER BY %s");
      qstrs.add(orderby);
    }
    if (limit != null) {
      queryFormat.append(" LIMIT %s");
      qstrs.add(String.valueOf(limit));
    }
    return String.format(queryFormat.toString(), qstrs.toArray(new Object[qstrs.size()]));
  }
}
