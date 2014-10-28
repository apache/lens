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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Accepts strings of all expressions and constructs HQL query.
 * 
 * Making this as an abstract class because it provides constructors without all
 * expressions being set.
 */
public abstract class SimpleHQLContext implements HQLContextInterface {

  public static Log LOG = LogFactory.getLog(SimpleHQLContext.class.getName());

  private String select;
  private String from;
  private String where;
  private String groupby;
  private String orderby;
  private String having;
  private Integer limit;

  SimpleHQLContext() {
  }

  SimpleHQLContext(String select, String from, String where, String groupby, String orderby, String having,
      Integer limit) {
    this.select = select;
    this.from = from;
    this.where = where;
    this.groupby = groupby;
    this.orderby = orderby;
    this.having = having;
    this.limit = limit;
  }

  SimpleHQLContext(String select, String groupby, String orderby, String having, Integer limit) {
    this.select = select;
    this.groupby = groupby;
    this.orderby = orderby;
    this.having = having;
    this.limit = limit;
  }

  /**
   * Set all missing expressions of HQL context.
   * 
   * Leaving this empty implementation for the case of all expressions being
   * passed in constructor. If other constructors are used the missing
   * expressions should be set here
   */
  protected void setMissingExpressions() throws SemanticException {
  }

  public String toHQL() throws SemanticException {
    setMissingExpressions();
    String qfmt = getQueryFormat();
    Object[] queryTreeStrings = getQueryTreeStrings();
    if (LOG.isDebugEnabled()) {
      LOG.debug("qfmt:" + qfmt + " Query strings: " + Arrays.toString(queryTreeStrings));
    }
    String baseQuery = String.format(qfmt, queryTreeStrings);
    return baseQuery;
  }

  private String[] getQueryTreeStrings() throws SemanticException {
    List<String> qstrs = new ArrayList<String>();
    qstrs.add(select);
    qstrs.add(from);
    if (!StringUtils.isBlank(where)) {
      qstrs.add(where);
    }
    if (!StringUtils.isBlank(groupby)) {
      qstrs.add(groupby);
    }
    if (!StringUtils.isBlank(having)) {
      qstrs.add(having);
    }
    if (!StringUtils.isBlank(orderby)) {
      qstrs.add(orderby);
    }
    if (limit != null) {
      qstrs.add(String.valueOf(limit));
    }
    return qstrs.toArray(new String[0]);
  }

  private final String baseQueryFormat = "SELECT %s FROM %s";

  private String getQueryFormat() {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append(baseQueryFormat);
    if (!StringUtils.isBlank(where)) {
      queryFormat.append(" WHERE %s");
    }
    if (!StringUtils.isBlank(groupby)) {
      queryFormat.append(" GROUP BY %s");
    }
    if (!StringUtils.isBlank(having)) {
      queryFormat.append(" HAVING %s");
    }
    if (!StringUtils.isBlank(orderby)) {
      queryFormat.append(" ORDER BY %s");
    }
    if (limit != null) {
      queryFormat.append(" LIMIT %s");
    }
    return queryFormat.toString();
  }

  public String getFrom() {
    return from;
  }

  public String getWhere() {
    return where;
  }

  public String getSelect() {
    return select;
  }

  public String getGroupby() {
    return groupby;
  }

  public String getHaving() {
    return having;
  }

  public String getOrderby() {
    return orderby;
  }

  public Integer getLimit() {
    return limit;
  }

  protected void setFrom(String from) {
    this.from = from;
  }

  protected void setWhere(String where) {
    this.where = where;
  }

  protected void setSelect(String select) {
    this.select = select;
  }

  protected void setGroupby(String groupby) {
    this.groupby = groupby;
  }

  protected void setHaving(String having) {
    this.having = having;
  }

  protected void setOrderby(String orderby) {
    this.orderby = orderby;
  }

}
