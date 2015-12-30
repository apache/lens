/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.lib.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.server.api.driver.LensResultSetMetadata;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.service.cli.ColumnDescriptor;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class MockLensResultSetMetadata extends LensResultSetMetadata {
  List<ColumnDescriptor> columns;

  @Override
  public List<ColumnDescriptor> getColumns() {
    return columns;
  }

  public static LensResultSetMetadata createMockedResultSet() {
    List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
    columns.add(new ColumnDescriptor(new FieldSchema("firstcol", "int", ""), 0));
    columns.add(new ColumnDescriptor(new FieldSchema("format(secondcol,2)", "string", ""), 1));
    columns.add(new ColumnDescriptor(new FieldSchema("thirdcol", "varchar(20)", ""), 2));
    columns.add(new ColumnDescriptor(new FieldSchema("fourthcol", "char(15)", ""), 3));
    columns.add(new ColumnDescriptor(new FieldSchema("fifthcol", "array<tinyint>", ""), 4));
    columns.add(new ColumnDescriptor(new FieldSchema("sixthcol", "struct<a:int,b:varchar(10)>", ""), 5));
    columns.add(new ColumnDescriptor(new FieldSchema("seventhcol", "map<int,char(10)>", ""), 6));
    return new MockLensResultSetMetadata(columns);
  }

  public static LensResultSetMetadata createMockedResultSetWithoutComma() {
    List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
    columns.add(new ColumnDescriptor(new FieldSchema("firstcol", "int", ""), 0));
    columns.add(new ColumnDescriptor(new FieldSchema("secondcol", "string", ""), 1));
    columns.add(new ColumnDescriptor(new FieldSchema("thirdcol", "varchar(20)", ""), 2));
    columns.add(new ColumnDescriptor(new FieldSchema("fourthcol", "char(15)", ""), 3));
    columns.add(new ColumnDescriptor(new FieldSchema("fifthcol", "array<tinyint>", ""), 4));
    columns.add(new ColumnDescriptor(new FieldSchema("sixthcol", "struct<a:int,b:varchar(10)>", ""), 5));
    columns.add(new ColumnDescriptor(new FieldSchema("seventhcol", "map<int,char(10)>", ""), 6));
    return new MockLensResultSetMetadata(columns);
  }
}
