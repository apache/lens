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
package org.apache.lens.ml.dao;

import org.apache.lens.ml.MLModel;
import org.apache.lens.ml.MLTestReport;
import org.apache.lens.ml.task.MLTask;

public class MLDBUtils {

  /**
   * Create table to store test report data
   */
  public void createTestReportTable() {

  }

  /**
   * Create table to store ML task workflow data
   */
  public void createMLTaskTable() {

  }

  /**
   * Create table to save ML Models
   */
  public void createMLModelTable() {

  }

  /**
   * Insert an ML Task into ml task table
   *
   * @param task
   */
  public void saveMLTask(MLTask task) {

  }

  /**
   * Get ML Task given its id
   *
   * @param taskID
   * @return
   */
  public MLTask getMLTask(String taskID) {
    return null;
  }

  /**
   * Insert test report into test report table
   *
   * @param testReport
   */
  public void saveTestReport(MLTestReport testReport) {

  }

  /**
   * Get test report given its ID
   *
   * @param testReportID
   * @return
   */
  public MLTestReport getTestReport(String testReportID) {
    return null;
  }

  /**
   * Insert model metadata into model table
   *
   * @param mlModel
   */
  public void saveMLModel(MLModel<?> mlModel) {

  }

  /**
   * Get model metadata given ID
   *
   * @param modelID
   * @return
   */
  public MLModel<?> getMLModel(String modelID) {
    return null;
  }

}
