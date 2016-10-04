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

package org.apache.lens.regression.config;

import java.io.IOException;
import java.lang.reflect.Method;

import javax.ws.rs.client.WebTarget;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.query.*;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.regression.client.SessionResourceTests;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;

import com.jcraft.jsch.JSchException;


public class ITSessionConfigTests extends BaseTestClass{

  WebTarget servLens;
  String sessionHandleString;

  private static String queryResultParentDirPath = "/tmp/lensreports";
  private String lensConfFilePath = lens.getServerDir() + "/conf/lens-site.xml";

  private static Logger logger = Logger.getLogger(SessionResourceTests.class);

  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException, JSchException, JAXBException, LensException {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
  }

  @AfterMethod(alwaysRun=true)
  public void restoreConfig() throws JSchException, IOException, JAXBException, LensException{
    qHelper.killQuery(null, "QUEUED", "all");
    qHelper.killQuery(null, "RUNNING", "all");
    if (sessionHandleString != null) {
      sHelper.closeSession();
    }
  }


  @DataProvider(name="query_provider")
  public Object[][] queryProvider(){
    String[][] query = { {QueryInventory.JDBC_DIM_QUERY}, {QueryInventory.HIVE_CUBE_QUERY},
                         {QueryInventory.HIVE_DIM_QUERY}, };
    return query;
  }


  // Test for lens.query.enable.persistent.resultset

  @Test(enabled=true, dataProvider="query_provider")
  public void testDisablePresistentResult(String queryString) throws Exception{

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(queryString).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    InMemoryQueryResult inmemory = (InMemoryQueryResult) qHelper.getResultSet(queryHandle, "0", "100");
    Assert.assertNotNull(inmemory);
  }

  @Test(enabled=true, dataProvider="query_provider")
  public void testPresistentResult(String queryString) throws Exception{

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(queryString).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    try{
      InMemoryQueryResult inmemory = (InMemoryQueryResult)qHelper.getResultSet(queryHandle, "0", "100");
      Assert.assertFalse(true);
    }catch(Exception e){
      logger.info(e.getMessage().toString());
    }

    PersistentQueryResult result = (PersistentQueryResult)qHelper.getResultSet(queryHandle, "0", "100");
    Assert.assertNotNull(result);
  }


  @Test(enabled=false)  //Issue with indriver persistence for local file path
  public void testPersistentResultsetIndriver() throws Exception{

    String parentDir = "file://" + queryResultParentDirPath;
    String hdfsoutPath = "temphdfsout";
    Util.runRemoteCommand("mkdir " + queryResultParentDirPath + "/" + hdfsoutPath);
    Util.runRemoteCommand("chmod -R 777 " + queryResultParentDirPath + "/"+ hdfsoutPath);

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SET_PARENT_DIR, parentDir);
    sHelper.setAndValidateParam(LensConfConstants.QUERY_HDFS_OUTPUT_PATH, hdfsoutPath);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    PersistentQueryResult result = (PersistentQueryResult)qHelper.getResultSet(queryHandle);
    String rowCount = Util.runRemoteCommand("cat " + queryResultParentDirPath + "/" + hdfsoutPath + "/"
        + queryHandle.toString() + "/* |wc -l");
    //Result file will have 2 lines extra, column header and footer saying number of rows.
    Assert.assertEquals(Integer.parseInt(rowCount.trim()), result.getNumRows()+2,
        "Result is not persisted in the given directory");
  }


  //Test for session properties lens.query.result.parent.dir

  @DataProvider(name="path_provider")
  public Object[][] pathProvider() throws JSchException, IOException{

    Util.runRemoteCommand("mkdir /tmp/temporary");
    Util.runRemoteCommand("chmod -R 777 /tmp/temporary");

    String[][] path = { {"file://" + queryResultParentDirPath, "local"},
                        {lens.getServerHdfsUrl() + queryResultParentDirPath, "hdfs"},
                        {"file:///tmp/temporary", "local"},
    };
    return path;
  }

  @Test(enabled=true, dataProvider="path_provider")
  public void testPersistentResultPath(String parentDir, String type) throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SET_PARENT_DIR, parentDir);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    PersistentQueryResult result = (PersistentQueryResult) qHelper.getResultSet(queryHandle);
    String rowCount = null;

    if (type.equalsIgnoreCase("hdfs")) {
      rowCount = Util.runRemoteCommand("hadoop fs -cat  " + parentDir + "/" + queryHandle + "* | gunzip | wc -l");
    } else {
      rowCount = Util.runRemoteCommand("cat " + parentDir.substring(7, parentDir.length()) + "/" + queryHandle
         + "* | gunzip | wc -l");
    }

    Assert.assertEquals(Integer.parseInt(rowCount.trim()), result.getNumRows()+2,
        "Result is not persisted in the given directory");
  }

  @Test(enabled=true, dataProvider="path_provider")
  public void testPersistentResultPathJDBCQuery(String parentDir, String type) throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SET_PARENT_DIR, parentDir);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");

    PersistentQueryResult result = (PersistentQueryResult) qHelper.getResultSet(queryHandle);
    String rowCount = null;

    if (type.equalsIgnoreCase("hdfs")) {
      rowCount = Util.runRemoteCommand("hadoop fs -cat " + parentDir + "/" + queryHandle + "* | gunzip | wc -l");
    } else {
      rowCount = Util.runRemoteCommand("cat " + parentDir.substring(7, parentDir.length()) + "/" + queryHandle
          + "* | gunzip | wc -l");
    }

    Assert.assertEquals(Integer.parseInt(rowCount.trim()), result.getNumRows()+2);

  }


  // Test for lens.query.result.split.multiple and lens.query.result.split.multiple.maxrows

  @Test(enabled=true, dataProvider="query_provider")
  public void testQueryResultSplit(String queryString) throws Exception{

    int splitSize = 1;
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SET_PARENT_DIR, lens.getServerHdfsUrl()
        + queryResultParentDirPath);
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE, "true");
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS, String.valueOf(splitSize));

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(queryString).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
    PersistentQueryResult result = (PersistentQueryResult)qHelper.getResultSet(queryHandle);

    logger.info(Util.runRemoteCommand("bash hadoop fs -get " + queryResultParentDirPath + "/" + queryHandle
        + ".zip  /tmp"));
    logger.info(Util.runRemoteCommand("cd /tmp; unzip " + queryHandle + ".zip"));
    String numberOfFiles = Util.runRemoteCommand("ls /tmp/" + queryHandle + "* | grep -v zip |wc -l");
    logger.info("number of files is-:" + numberOfFiles.trim());
    int numFiles = Integer.parseInt(numberOfFiles.trim());
    Assert.assertEquals(numFiles, result.getNumRows().intValue(), "Files are not splitting");

    for(int i = 0; i < numFiles; i++) {
      String rowsPerFile = Util.runRemoteCommand("cd /tmp;ls " + queryHandle + "_part-" + String.valueOf(i)
          + ".csv |wc -l");
      Assert.assertEquals(rowsPerFile.trim(), String.valueOf(splitSize), "Maxrows property not followed");
    }
  }

  // Test for property lens.cube.query.fail.if.data.partial
  @Test(enabled=true)
  public void testQueryFailIfDataPartial() throws Exception{

    sHelper.setAndValidateParam(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "true");
    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.NO_PARTITION_HIVE_CUBE_QUERY).getData();
    Assert.assertNull(queryHandle);

    sHelper.setAndValidateParam(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
    queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.NO_PARTITION_HIVE_CUBE_QUERY).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed!!");
  }


 /*
  * Test for property lens.query.output.write.header and lens.query.output.header
 */

  @Test(enabled=true, dataProvider="query_provider")
  public void testQueryOutputHeader(String queryString) throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_OUTPUT_WRITE_HEADER, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_OUTPUT_HEADER, "Query Result");
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SET_PARENT_DIR, "file://" + queryResultParentDirPath);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(queryString).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed!!");

    PersistentQueryResult result = (PersistentQueryResult)qHelper.getResultSet(queryHandle);
    Util.runRemoteCommand("gzip -d " + queryResultParentDirPath + "/" + queryHandle + ".csv.gz");

    String content = Util.runRemoteCommand("cat " + queryResultParentDirPath + "/" + queryHandle + ".csv");
    String[] lines = content.split("\n");
    Assert.assertEquals(lines[0].trim(), "Query Result", "Header incorrect");

    sHelper.setAndValidateParam(LensConfConstants.QUERY_OUTPUT_WRITE_HEADER, "false");

    queryHandle = (QueryHandle) qHelper.executeQuery(queryString).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed!!");

    PersistentQueryResult result1 = (PersistentQueryResult)qHelper.getResultSet(queryHandle);
    Util.runRemoteCommand("gzip -d " + queryResultParentDirPath + "/" + queryHandle + ".csv.gz");

    content = Util.runRemoteCommand("cat " + queryResultParentDirPath + "/" + queryHandle + ".csv");
    lines = content.split("\n");
    Assert.assertNotEquals(lines[0].trim(), "Query Result", "Header should not be printed");
  }

  /*
   * Test for property lens.query.output.write.footer and lens.query.output.footer
  */

  @Test(enabled=true, dataProvider="query_provider")
  public void testQueryOutputFooter(String queryString) throws Exception {

    sHelper.setAndValidateParam(LensConfConstants.QUERY_OUTPUT_FOOTER, "CopyRight");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_OUTPUT_WRITE_FOOTER, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE, "false");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    sHelper.setAndValidateParam(LensConfConstants.RESULT_SET_PARENT_DIR, "file://" + queryResultParentDirPath);

    QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(queryString).getData();
    LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed!!");

    PersistentQueryResult result = (PersistentQueryResult)qHelper.getResultSet(queryHandle);
    Util.runRemoteCommand("gzip -d " + queryResultParentDirPath + "/" + queryHandle + ".csv.gz");

    String content = Util.runRemoteCommand("cat " + queryResultParentDirPath + "/" + queryHandle + ".csv");
    String[] lines = content.split("\n");
    Assert.assertEquals(lines[result.getNumRows()+1].trim(), "CopyRight", "Footer incorrect");

    sHelper.setAndValidateParam(LensConfConstants.QUERY_OUTPUT_WRITE_FOOTER, "false");

    queryHandle = (QueryHandle) qHelper.executeQuery(queryString).getData();
    lensQuery = qHelper.waitForCompletion(queryHandle);
    Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed!!");

    PersistentQueryResult result1 = (PersistentQueryResult)qHelper.getResultSet(queryHandle);
    Util.runRemoteCommand("gzip -d " + queryResultParentDirPath + "/" + queryHandle + ".csv.gz");

    content = Util.runRemoteCommand("cat " + queryResultParentDirPath + "/" + queryHandle + ".csv");
    lines = content.split("\n");
    Assert.assertEquals(lines.length, (result1.getNumRows().intValue())+1, "Footer shouldn't be coming");
  }

}



