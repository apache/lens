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


import java.util.List;

import org.apache.lens.server.api.driver.LensResultSetMetadata;

import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;

import com.google.common.collect.Lists;
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
    return new MockLensResultSetMetadata(Lists.newArrayList(
      new ColumnDescriptor("firstcol", "", new TypeDescriptor("int"), 0),
      new ColumnDescriptor("format(secondcol,2)", "", new TypeDescriptor("string"), 1),
      new ColumnDescriptor("thirdcol", "", new TypeDescriptor("varchar(20)"), 2),
      new ColumnDescriptor("fourthcol", "", new TypeDescriptor("char(15)"), 3),
      new ColumnDescriptor("fifthcol", "", new TypeDescriptor("array<tinyint>"), 4),
      new ColumnDescriptor("sixthcol", "", new TypeDescriptor("struct<a:int,b:varchar(10)>"), 5),
      new ColumnDescriptor("seventhcol", "", new TypeDescriptor("map<int,char(10)>"), 5)
    ));
  }

  public static LensResultSetMetadata createMockedResultSetWithoutComma() {
    return new MockLensResultSetMetadata(Lists.newArrayList(
      new ColumnDescriptor("firstcol", "", new TypeDescriptor("int"), 0),
      new ColumnDescriptor("secondcol", "", new TypeDescriptor("string"), 1),
      new ColumnDescriptor("thirdcol", "", new TypeDescriptor("varchar(20)"), 2),
      new ColumnDescriptor("fourthcol", "", new TypeDescriptor("char(15)"), 3),
      new ColumnDescriptor("fifthcol", "", new TypeDescriptor("array<tinyint>"), 4),
      new ColumnDescriptor("sixthcol", "", new TypeDescriptor("struct<a:int,b:varchar(10)>"), 5),
      new ColumnDescriptor("seventhcol", "", new TypeDescriptor("map<int,char(10)>"), 6)
    ));
  }
}
