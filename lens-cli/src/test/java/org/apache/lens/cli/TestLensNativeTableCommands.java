package org.apache.lens.cli;

import org.apache.lens.cli.commands.LensNativeTableCommands;
import org.apache.lens.client.LensClient;
import org.apache.lens.server.LensTestUtil;
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
    try {
      LensClient client = new LensClient();
      LensNativeTableCommands command = new LensNativeTableCommands();
      command.setClient(client);
      LOG.debug("Starting to test nativetable commands");
      String tblList = command.showNativeTables();
      Assert.assertFalse(tblList.contains("test_native_table_command"));
      LensTestUtil.createHiveTable("test_native_table_command");
      tblList = command.showNativeTables();
      Assert.assertTrue(tblList.contains("test_native_table_command"));

      String desc = command.describeNativeTable("test_native_table_command");
      LOG.info(desc);
      Assert.assertTrue(desc.contains("col1"));
      Assert.assertTrue(desc.contains("pcol1"));
      Assert.assertTrue(desc.contains("MANAGED_TABLE"));
      Assert.assertTrue(desc.contains("test.hive.table.prop"));
    } finally {
      LensTestUtil.dropHiveTable("test_native_table_command");

    }
  }
}
