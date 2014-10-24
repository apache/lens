package org.apache.lens.cli;

import org.apache.lens.cli.commands.LensCubeCommands;
import org.apache.lens.client.LensClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URL;

/**
 * The Class TestLensCubeCommands.
 */
public class TestLensCubeCommands extends LensCliApplicationTest {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(TestLensCubeCommands.class);

  /**
   * Test cube commands.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testCubeCommands() throws Exception {
    LensClient client = new LensClient();
    LensCubeCommands command = new LensCubeCommands();
    command.setClient(client);
    LOG.debug("Starting to test cube commands");
    URL cubeSpec = TestLensCubeCommands.class.getClassLoader().getResource("sample-cube.xml");
    String cubeList = command.showCubes();
    Assert.assertFalse(cubeList.contains("sample_cube"));
    command.createCube(new File(cubeSpec.toURI()).getAbsolutePath());
    cubeList = command.showCubes();
    Assert.assertTrue(cubeList.contains("sample_cube"));

    testUpdateCommand(new File(cubeSpec.toURI()), command);
    command.dropCube("sample_cube");
    cubeList = command.showCubes();
    Assert.assertFalse(cubeList.contains("sample_cube"));
  }

  /**
   * Test update command.
   *
   * @param f
   *          the f
   * @param command
   *          the command
   */
  private void testUpdateCommand(File f, LensCubeCommands command) {
    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();

      xmlContent = xmlContent.replace("<properties name=\"sample_cube.prop\" value=\"sample\" />\n",
          "<properties name=\"sample_cube.prop\" value=\"sample\" />"
              + "\n<properties name=\"sample_cube.prop1\" value=\"sample1\" />\n");

      File newFile = new File("/tmp/sample_cube1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeCube("sample_cube");
      LensClient client = command.getClient();
      LOG.debug(desc);
      String propString = "name : sample_cube.prop  value : sample";
      String propString1 = "name : sample_cube.prop1  value : sample1";

      Assert.assertTrue(desc.contains(propString));

      command.updateCube("sample_cube /tmp/sample_cube1.xml");
      desc = command.describeCube("sample_cube");
      LOG.debug(desc);
      Assert.assertTrue(desc.contains(propString));
      Assert.assertTrue(desc.contains(propString1));

      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Testing update cube failed with exception" + t.getMessage());
    }
  }
}
