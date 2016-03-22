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
package org.apache.lens.server.metastore;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.util.HashMap;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.metastore.CubeMetastoreService;

import org.apache.commons.io.FileUtils;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.glassfish.jersey.test.TestProperties;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@Test(groups = "unit-test")
public class TestDatabaseService extends LensJerseyTest {
  CubeMetastoreServiceImpl metastoreService;
  LensSessionHandle lensSessionId;
  String rootPath = null;


  private void assertSuccess(APIResult result) {
    assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED, String.valueOf(result));
  }

  @BeforeMethod
  public void setUp() throws Exception {
    super.setUp();
    rootPath = getServerConf().get(LensConfConstants.DATABASE_RESOURCE_DIR);
    metastoreService = LensServices.get().getService(CubeMetastoreService.NAME);
    lensSessionId = metastoreService.openSession("foo", "bar", new HashMap<String, String>());
  }

  @AfterMethod
  public void tearDown() throws Exception {
    metastoreService.closeSession(lensSessionId);
    super.tearDown();
  }

  @Override
  protected Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    return new MetastoreApp();
  }

  private String getCurrentDatabase(MediaType mediaType) throws Exception {
    return target().path("metastore").path("databases/current")
      .queryParam("sessionid", lensSessionId).request(mediaType).get(String.class);
  }

  private FormDataMultiPart getFormData(MediaType mediaType) {
    FormDataMultiPart mp = null;
    try {

      mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), "jar"));

      File file = new File("target/testjars/serde.jar");
      log.debug("uploading file path : " + file.getAbsolutePath() + "|size = " + file.length());
      final FormDataContentDisposition dispo = FormDataContentDisposition
        .name("file")
        .fileName("test.jar")
        .size(file.length())
        .build();

      FileDataBodyPart filePart = new FileDataBodyPart("file", file);
      filePart.setContentDisposition(dispo);
      mp.bodyPart(filePart);
    } catch (Exception e) {
      log.error("Error in getting form data", e);
    }

    return mp;

  }

  /**
   * Test case when no db folder exists
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUpload(MediaType mediaType) throws Exception {
    String dbName = "db1" + "_" + mediaType.getSubtype();

    // create
    APIResult result = target().path("metastore").path("databases")
      .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
        .class);
    assertNotNull(result);
    assertSuccess(result);

    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");

    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);


    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);


    APIResult resultUpd = target().path("metastore").path("databases/jar").
      queryParam("sessionid", lensSessionId).request(mediaType)
      .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertEquals(resultUpd.getMessage(), "Database resource location does not exist. Database jar can't be uploaded");

  }


  /**
   * Test case when db folder exists & jar_order file present ( Existing flow )
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUploadWithJarOrderFileInDbFolder(MediaType mediaType) throws Exception {
    String dbName = "db2" + "_" + mediaType.getSubtype() + "_" + System.currentTimeMillis();

    // create
    APIResult result = target().path("metastore").path("databases")
      .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
        .class);
    assertNotNull(result);
    assertSuccess(result);

    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");

    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // Create DB folder
    File dbFolder = new File("target/resources/" + dbName);
    dbFolder.mkdirs();

    File dbFolderJarOrder = new File("target/resources/" + dbName + File.separator + "jar_order");
    dbFolderJarOrder.createNewFile();


    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);


    APIResult resultUpd = target().path("metastore").path("databases/jar").
      queryParam("sessionid", lensSessionId).request(mediaType)
      .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertEquals(resultUpd.getMessage(), "Database jar_order file exist. Database jar can't be uploaded");

    cleanUp(dbFolder);
  }


  /**
   * Test case when db folder exists & jar_order file NOT present & db_uploading.jar present.
   * This restricts single upload ant any time.
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUploadWithDbUploadingJarInFolder(MediaType mediaType) throws Exception {
    String dbName = "db3" + "_" + mediaType.getSubtype() + "_" + System.currentTimeMillis();

    // create
    APIResult result = target().path("metastore").path("databases")
      .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
        .class);
    assertNotNull(result);
    assertSuccess(result);

    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");

    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // Create DB folder
    File dbFolder = new File("target/resources/" + dbName);
    dbFolder.mkdirs();

    File dbFolderJarOrder = new File("target/resources/" + dbName + File.separator + dbName + "_uploading.jar");
    dbFolderJarOrder.createNewFile();

    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);


    APIResult resultUpd = target().path("metastore").path("databases/jar").
      queryParam("sessionid", lensSessionId).request(mediaType)
      .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertEquals(resultUpd.getMessage(), "Database jar file upload in progress . Database jar can't be uploaded."
      + " Try later!");

    cleanUp(dbFolder);
  }


  /**
   * Test case when db folder exists & jar_order file NOT present & db_uploading.jar NOT present.
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUploadWithNoJarOrderInFolder(MediaType mediaType) throws Exception {
    String dbName = "db4" + "_" + mediaType.getSubtype() + "_" + System.currentTimeMillis();

    // create
    APIResult result = target().path("metastore").path("databases")
      .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
        .class);
    assertNotNull(result);
    assertSuccess(result);

    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");

    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // Create DB folder
    File dbFolder = new File("target/resources/" + dbName);
    dbFolder.mkdirs();

    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);


    APIResult resultUpd = target().path("metastore").path("databases/jar").
      queryParam("sessionid", lensSessionId).request(mediaType)
      .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertEquals(resultUpd.getStatus(), APIResult.Status.SUCCEEDED);

    cleanUp(dbFolder);
  }

  /**
   * Test case when db folder exists & jar_order file NOT present & db_uploading.jar NOT present and with existing jars.
   *
   * @param mediaType
   * @throws Exception
   */
  @Test(dataProvider = "mediaTypeData")
  public void testJarUploadWithExistingJarsInFolder(MediaType mediaType) throws Exception {
    String dbName = "db5" + "_" + mediaType.getSubtype() + "_" + System.currentTimeMillis();

    // create
    APIResult result = target().path("metastore").path("databases")
      .queryParam("sessionid", lensSessionId).request(mediaType).post(getEntityForString(dbName, mediaType), APIResult
        .class);
    assertNotNull(result);
    assertSuccess(result);

    // set
    WebTarget dbTarget = target().path("metastore").path("databases/current");

    result = dbTarget.queryParam("sessionid", lensSessionId).request(mediaType)
      .put(getEntityForString(dbName, mediaType), APIResult.class);
    assertNotNull(result);
    assertSuccess(result);

    // Create DB folder
    File dbFolder = new File("target/resources/" + dbName);
    dbFolder.mkdirs();

    File dbFolderJarOrder1 = new File("target/resources/" + dbName + File.separator + dbName + "_1.jar");
    dbFolderJarOrder1.createNewFile();

    File dbFolderJarOrder2 = new File("target/resources/" + dbName + File.separator + dbName + "_2.jar");
    dbFolderJarOrder2.createNewFile();

    FormDataMultiPart mp = getFormData(mediaType);
    MultiPart multiPart = new MultiPart();
    multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);


    APIResult resultUpd = target().path("metastore").path("databases/jar").
      queryParam("sessionid", lensSessionId).request(mediaType)
      .post(Entity.entity(mp, multiPart.getMediaType()), APIResult.class);
    log.debug(resultUpd.getStatus() + " " + resultUpd);
    assertEquals(resultUpd.getStatus(), APIResult.Status.SUCCEEDED);

    cleanUp(dbFolder);
  }

  private void cleanUp(File f) {
    try {
      FileUtils.deleteDirectory(f);
    } catch (Exception e) {
      log.error("Error cleaning directory", e);
    }
  }

}
