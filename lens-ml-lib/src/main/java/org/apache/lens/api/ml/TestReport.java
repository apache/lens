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
package org.apache.lens.api.ml;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * The Class TestReport.
 */
@XmlRootElement
/**
 * Instantiates a new test report.
 *
 * @param testTable
 *          the test table
 * @param outputTable
 *          the output table
 * @param outputColumn
 *          the output column
 * @param labelColumn
 *          the label column
 * @param featureColumns
 *          the feature columns
 * @param algorithm
 *          the algorithm
 * @param modelID
 *          the model id
 * @param reportID
 *          the report id
 * @param queryID
 *          the query id
 */
@AllArgsConstructor
/**
 * Instantiates a new test report.
 */
@NoArgsConstructor
public class TestReport {

  /** The test table. */
  @XmlElement
  @Getter
  private String testTable;

  /** The output table. */
  @XmlElement
  @Getter
  private String outputTable;

  /** The output column. */
  @XmlElement
  @Getter
  private String outputColumn;

  /** The label column. */
  @XmlElement
  @Getter
  private String labelColumn;

  /** The feature columns. */
  @XmlElement
  @Getter
  private String featureColumns;

  /** The algorithm. */
  @XmlElement
  @Getter
  private String algorithm;

  /** The model id. */
  @XmlElement
  @Getter
  private String modelID;

  /** The report id. */
  @XmlElement
  @Getter
  private String reportID;

  /** The query id. */
  @XmlElement
  @Getter
  private String queryID;

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Input test table: ").append(testTable).append('\n');
    builder.append("Algorithm: ").append(algorithm).append('\n');
    builder.append("Report id: ").append(reportID).append('\n');
    builder.append("Model id: ").append(modelID).append('\n');
    builder.append("Grill Query id: ").append(queryID).append('\n');
    builder.append("Feature columns: ").append(featureColumns).append('\n');
    builder.append("Labelled column: ").append(labelColumn).append('\n');
    builder.append("Predicted column: ").append(outputColumn).append('\n');
    builder.append("Test output table: ").append(outputTable).append('\n');
    return builder.toString();
  }
}
