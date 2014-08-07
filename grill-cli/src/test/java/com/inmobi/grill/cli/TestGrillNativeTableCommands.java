package com.inmobi.grill.cli;
/*
 * #%L
 * Grill CLI
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.inmobi.grill.cli.commands.GrillNativeTableCommands;
import com.inmobi.grill.client.GrillClient;
import com.inmobi.grill.server.GrillTestUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGrillNativeTableCommands extends GrillCliApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestGrillNativeTableCommands.class);

  @Test
  public void testNativeTableCommands() throws Exception {
    try {
    GrillClient client = new GrillClient();
    GrillNativeTableCommands command = new GrillNativeTableCommands();
    command.setClient(client);
    LOG.debug("Starting to test nativetable commands");
    String tblList = command.showNativeTables();
    Assert.assertFalse(
        tblList.contains("test_native_table_command"));
    GrillTestUtil.createHiveTable("test_native_table_command");
    tblList = command.showNativeTables();
    Assert.assertTrue(
        tblList.contains("test_native_table_command"));

    String desc = command.describeNativeTable("test_native_table_command");
    LOG.info(desc);
    Assert.assertTrue(desc.contains("col1"));
    Assert.assertTrue(desc.contains("pcol1"));
    Assert.assertTrue(desc.contains("MANAGED_TABLE"));
    Assert.assertTrue(desc.contains("test.hive.table.prop"));
    } finally {
      GrillTestUtil.dropHiveTable("test_native_table_command");
      
    }
  }
}
