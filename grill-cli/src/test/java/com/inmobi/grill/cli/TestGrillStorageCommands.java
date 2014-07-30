package com.inmobi.grill.cli;


import com.inmobi.grill.cli.commands.GrillStorageCommands;
import com.inmobi.grill.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URL;

public class TestGrillStorageCommands extends GrillCliApplicationTest {

  private static GrillStorageCommands command;
  private static final Logger LOG = LoggerFactory.getLogger(TestGrillStorageCommands.class);


  @Test
  public void testStorageCommands() {
    addLocalStorage("local_storage_test");
    testUpdateStorage("local_storage_test");
    dropStorage("local_storage_test");

  }

  private static GrillStorageCommands getCommand() {
    if(command == null) {
      GrillClient client = new GrillClient();
      command = new GrillStorageCommands();
      command.setClient(client);
    }
    return command;
  }

  public static void dropStorage(String storageName) {
    String storageList;
    GrillStorageCommands command = getCommand();
    command.dropStorage(storageName);
    storageList = command.getStorages();
    Assert.assertFalse( storageList.contains(storageName),"Storage list contains "+storageName);
  }

  public synchronized static void addLocalStorage(String storageName) {
    GrillStorageCommands command = getCommand();
    URL storageSpec =
        TestGrillStorageCommands.class.getClassLoader().getResource("local-storage.xml");
    File newFile = new File("/tmp/local-"+storageName+".xml");
    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(storageSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();

      xmlContent = xmlContent.replace("name=\"local\"",
          "name=\""+storageName+"\"");

      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();
    LOG.debug("Using Storage spec from file : " + newFile.getAbsolutePath());
    String storageList = command.getStorages();
    Assert.assertFalse(storageList.contains(storageName),
        " Storage list contains "+storageName + " storage list is  "
            + storageList + " file used is " + newFile.getAbsolutePath());
    command.createStorage(newFile.getAbsolutePath());
    storageList = command.getStorages();
    Assert.assertTrue(storageList.contains(storageName));
    } catch (Exception e) {
      Assert.fail("Unable to add storage " + storageName);
    } finally {
      newFile.delete();
    }
  }

  private void testUpdateStorage(String storageName) {

    try {
      GrillStorageCommands command = getCommand();
      URL storageSpec =
          TestGrillStorageCommands.class.getClassLoader().getResource("local-storage.xml");
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(storageSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();
      xmlContent = xmlContent.replace("name=\"local\"",
          "name=\""+storageName+"\"");
      xmlContent = xmlContent.replace("<properties name=\"storage.url\" value=\"file:///\"/>\n",
          "<properties name=\"storage.url\" value=\"file:///\"/>" +
              "\n<properties name=\"sample_cube.prop1\" value=\"sample1\" />\n");

      File newFile = new File("/tmp/"+storageName+".xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeStorage(storageName);
      LOG.debug(desc);
      Assert.assertTrue(desc.contains("storage.url=file:///"));

      command.updateStorage(storageName+" /tmp/local-storage1.xml");
      desc = command.describeStorage(storageName);
      LOG.debug(desc);
      Assert.assertTrue(desc.contains("storage.url=file:///"));
      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Testing update storage failed with exception" + t.getMessage());
    }
  }


}
