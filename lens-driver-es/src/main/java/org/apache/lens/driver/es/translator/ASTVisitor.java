/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es.translator;

import org.apache.lens.driver.es.exceptions.InvalidQueryException;

/**
 * The visitor interface for ASTInorderTraversal
 * It covers only simple queries (no joins or sub queries)
 * Also it does not handle complicated expressions
 */
public interface ASTVisitor {

  enum OrderBy {ASC, DESC}

  void visitSimpleSelect(String columnName, String alias);

  void visitAggregation(String aggregationType, String columnName, String alias) throws InvalidQueryException;

  void visitFrom(String database, String table);

  void visitCriteria(ASTCriteriaVisitor visitedSubTree);

  void visitGroupBy(String colName);

  void visitOrderBy(String colName, OrderBy orderBy);

  void visitLimit(int limit);

  void completeVisit();

  void visitAllCols();
}
