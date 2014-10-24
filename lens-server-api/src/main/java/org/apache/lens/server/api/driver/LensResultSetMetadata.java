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
package org.apache.lens.server.api.driver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;
import org.apache.lens.api.query.QueryResultSetMetadata;
import org.apache.lens.api.query.ResultColumn;

/**
 * The Class LensResultSetMetadata.
 */
public abstract class LensResultSetMetadata {

  public abstract List<ColumnDescriptor> getColumns();

  /**
   * To query result set metadata.
   *
   * @return the query result set metadata
   */
  public QueryResultSetMetadata toQueryResultSetMetadata() {
    List<ResultColumn> result = new ArrayList<ResultColumn>();
    for (ColumnDescriptor col : getColumns()) {
      result.add(new ResultColumn(col.getName(), col.getType().getName()));
    }
    return new QueryResultSetMetadata(result);
  }

  /**
   * Gets the qualified type name.
   *
   * @param typeDesc
   *          the type desc
   * @return the qualified type name
   */
  public static String getQualifiedTypeName(TypeDescriptor typeDesc) {
    if (typeDesc.getType().isQualifiedType()) {
      switch (typeDesc.getType()) {
      case VARCHAR_TYPE:
        return VarcharTypeInfo.getQualifiedName(typeDesc.getTypeName(),
            typeDesc.getTypeQualifiers().getCharacterMaximumLength()).toLowerCase();
      case CHAR_TYPE:
        return CharTypeInfo.getQualifiedName(typeDesc.getTypeName(),
            typeDesc.getTypeQualifiers().getCharacterMaximumLength()).toLowerCase();
      case DECIMAL_TYPE:
        return DecimalTypeInfo.getQualifiedName(typeDesc.getTypeQualifiers().getPrecision(),
            typeDesc.getTypeQualifiers().getScale()).toLowerCase();
      }
    }
    return typeDesc.getTypeName().toLowerCase();
  }
}
