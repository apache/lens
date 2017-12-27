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
package org.apache.lens.server.api.metastore;

import java.util.Date;
import java.util.List;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.metastore.*;
import org.apache.lens.server.api.LensService;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Server api for OLAP Cube Databse.
 */
public interface DatabaseResourcesService extends LensService {

  /** The constant NAME */
  String NAME = "database";


  /**
   * refresh database resources
   *
   * @param dbName
   * @throws LensException
   */
  void refreshDatabaseResources(String dbName) throws LensException;


}

