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

import java.util.HashMap;

import org.apache.lens.cli.commands.LensNativeTableCommands;
import org.apache.lens.client.LensClient;
import org.apache.lens.server.LensServerTestUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class TestLensNativeTableCommands.
 */
public class TestLensNativeTableCommands extends LensCliApplicationTest {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensNativeTableCommands.class);

  /**
   * Test native table commands.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testNativeTableCommands() throws Exception {
    LensClient client = new LensClient();
    try {
      LensNativeTableCommands command = new LensNativeTableCommands();
      command.setClient(client);
      LOG.debug("Starting to test nativetable commands");
      String tblList = command.showNativeTables();
      Assert.assertFalse(tblList.contains("test_native_table_command"));
      LensServerTestUtil.createHiveTable("test_native_table_command", new HashMap<String, String>());
      tblList = command.showNativeTables();
      Assert.assertTrue(tblList.contains("test_native_table_command"));

      String desc = command.describeNativeTable("test_native_table_command");
      LOG.info(desc);
      Assert.assertTrue(desc.contains("col1"));
      Assert.assertTrue(desc.contains("pcol1"));
      Assert.assertTrue(desc.contains("MANAGED_TABLE"));
      Assert.assertTrue(desc.contains("test.hive.table.prop"));
    } finally {
      LensServerTestUtil.dropHiveTable("test_native_table_command");
      client.closeConnection();
    }
  }
}
