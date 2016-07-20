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

package org.apache.lens.regression.core.constants;

public class MetastoreURL {

  private MetastoreURL() {

  }

  public static final String METASTORE_BASE_URL = "/metastore";
  public static final String METASTORE_CUBES_URL = METASTORE_BASE_URL + "/cubes";
  public static final String METASTORE_DATABASES_URL = METASTORE_BASE_URL + "/databases";
  public static final String METASTORE_DATABASES_CURRENT_URL = METASTORE_DATABASES_URL + "/current";
  public static final String METASTORE_DIMENSIONS_URL = METASTORE_BASE_URL + "/dimensions";
  public static final String METASTORE_DIMTABLES_URL = METASTORE_BASE_URL + "/dimtables";
  public static final String METASTORE_FACTS_URL = METASTORE_BASE_URL + "/facts";
  public static final String METASTORE_FLATTENED_URL = METASTORE_BASE_URL + "/flattened";
  public static final String METASTORE_NATIVETABLES_URL = METASTORE_BASE_URL + "/nativetables";
  public static final String METASTORE_STORAGES_URL = METASTORE_BASE_URL + "/storages";
}
