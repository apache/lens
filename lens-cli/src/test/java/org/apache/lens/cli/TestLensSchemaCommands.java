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
package org.apache.lens.cli;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * The Class TestLensStorageCommands.
 */
public class TestLensSchemaCommands extends LensCLITest {

  @Test
  public void testCreateSchema() throws Throwable {
    String schemaDirectory = TestLensSchemaCommands.class.getClassLoader().getResource("schema").getFile();
    String dbName = "schema_command_db";
    try {
      execute("schema --db " + dbName + " --path " + schemaDirectory, null);
      assertTrue(((String) execute("show databases")).contains(dbName));
      execute("show storages", "local");
      execute("show dimensions", "test_detail\ntest_dim");
      execute("show cubes", "sample_cube\ncube_with_no_weight_facts");
      assertTrue(((String) execute("show dimtables")).contains("dim_table"));
      assertTrue(((String) execute("show facts")).contains("fact1"));
      execute("show segmentations", "seg1");
    } finally {
      execute("drop database --db " + dbName + " --cascade", "succeeded");
      assertFalse(((String) execute("show databases")).contains(dbName));
    }
  }
}
