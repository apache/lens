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
/*
 *
 */
package org.apache.lens.api.query;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.*;

/**
 * The Class InMemoryQueryResult.
 */
@XmlRootElement
/**
 * Instantiates a new in memory query result.
 *
 * @param rows
 *          the rows
 */
@AllArgsConstructor
/**
 * Instantiates a new in memory query result.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InMemoryQueryResult extends QueryResult {
  public static final String DECLARATION = "Result available in memory, attaching here: \n\n";
  /**
   * The rows.
   */
  @XmlElementWrapper
  @Getter
  private List<ResultRow> rows = new ArrayList<>();

  public String toPrettyString() {
    StringBuilder b = new StringBuilder();
    b.append(DECLARATION);
    int numRows = 0;
    for (ResultRow row : getRows()) {
      for (Object col : row.getValues()) {
        b.append(col).append("\t");
      }
      numRows++;
      b.append("\n");
    }
    b.append(numRows).append(" rows ");
    return b.toString();
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
