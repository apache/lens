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
package org.apache.lens.cli.table;


import java.util.Collection;

import org.springframework.shell.support.table.Table;
import org.springframework.shell.support.table.TableHeader;
import org.springframework.shell.support.table.TableRenderer;

import lombok.Data;

@Data
public class CollectionTable<T> {
  private final Collection<T> collection;
  private final RowProvider<T> provider;
  private final String[] header;

  public CollectionTable(Collection<T> collection, RowProvider<T> provider, String... header) {
    this.collection = collection;
    this.provider = provider;
    this.header = header;
  }

  interface RowProvider<T> {
    String[][] getRows(T element);
  }

  @Override
  public String toString() {
    Table table = new Table();
    for (int i = 0; i < header.length; i++) {
      table.addHeader(i + 1, new TableHeader(header[i]));
    }
    for (T element : collection) {
      for (String[] row : provider.getRows(element)) {
        table.addRow(row);
      }
    }
    return TableRenderer.renderTextTable(table);
  }
}
