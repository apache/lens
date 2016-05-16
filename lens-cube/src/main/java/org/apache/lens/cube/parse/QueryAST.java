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

import org.apache.hadoop.hive.ql.parse.ASTNode;

public interface QueryAST {

  String getSelectString();

  String getFromString();

  String getWhereString();

  String getHavingString();

  String getOrderByString();

  String getGroupByString();

  Integer getLimitValue();

  void setLimitValue(Integer integer);

  /**
   * @return the selectAST
   */

  ASTNode getSelectAST();

  /**
   * @param selectAST the selectAST to set
   */

  void setSelectAST(ASTNode selectAST);

  /**
   * @return the whereAST
   */

  ASTNode getWhereAST();

  /**
   * @param whereAST the whereAST to set
   */

  void setWhereAST(ASTNode whereAST);

  /**
   * @return the havingAST
   */

  ASTNode getHavingAST();

  /**
   * @param havingAST the havingAST to set
   */

  void setHavingAST(ASTNode havingAST);

  ASTNode getGroupByAST();

  void setGroupByAST(ASTNode havingAST);

  ASTNode getJoinAST();

  ASTNode getOrderByAST();

  void setOrderByAST(ASTNode node);
}
