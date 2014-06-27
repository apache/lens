package com.inmobi.grill.cli;

import com.inmobi.grill.cli.commands.GrillCubeCommands;
import com.inmobi.grill.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;

public class TestGrillCubeCommands extends GrillCliApplicationTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestGrillCubeCommands.class);

  @Test
  public void testCubeCommands() {
    GrillClient client = new GrillClient();
    GrillCubeCommands command = new GrillCubeCommands();
    command.setClient(client);
    LOG.debug("Starting to test cube commands");
    File f = TestUtil.getPath("sample-cube.xml");

    String cubeList = command.showCubes();
    Assert.assertFalse(
        cubeList.contains("sample_cube"));
    command.createCube(f.getAbsolutePath());
    cubeList = command.showCubes();
    Assert.assertTrue(
        cubeList.contains("sample_cube"));

    testUpdateCommand(f, command);
    command.dropCube("sample_cube");
    cubeList = command.showCubes();
    Assert.assertFalse(
        cubeList.contains("sample_cube"));
  }

  private void testUpdateCommand(File f, GrillCubeCommands command) {
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
          "<properties name=\"sample_cube.prop\" value=\"sample\" />" +
              "\n<properties name=\"sample_cube.prop1\" value=\"sample1\" />\n");

      File newFile = new File("/tmp/sample_cube1.xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeCube("sample_cube");
      LOG.debug(desc);
      Assert.assertTrue(
          desc.contains("sample_cube.prop=sample"));

      command.updateCube("sample_cube /tmp/sample_cube1.xml");
      desc = command.describeCube("sample_cube");
      LOG.debug(desc);
      Assert.assertTrue(
          desc.contains("sample_cube.prop=sample"));

      Assert.assertTrue(
          desc.contains("sample_cube.prop1=sample1"));

      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Testing update cube failed with exception" + t.getMessage());
    }
  }
}
