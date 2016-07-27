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
package org.apache.lens.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.server.api.LensConfConstants;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class LensServerTestUtil.
 */
@Slf4j
public final class LensServerTestUtil {

  public static final String DB_WITH_JARS = "test_db_static_jars";
  public static final String DB_WITH_JARS_2 = "test_db_static_jars_2";
  private LensServerTestUtil() {

  }

  /**
   * Creates the table.
   *
   * @param tblName       the tbl name
   * @param parent        the parent
   * @param lensSessionId the lens session id
   * @param schemaStr     the schema string, with surrounding parenthesis.
   * @throws InterruptedException the interrupted exception
   */
  public static void createTable(String tblName, WebTarget parent, LensSessionHandle lensSessionId, String schemaStr,
    MediaType mt)
    throws InterruptedException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    final WebTarget target = parent.path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    String createTable = "CREATE TABLE IF NOT EXISTS " + tblName + schemaStr;

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), createTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));

    final QueryHandle handle = target.request(mt)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
            new GenericType<LensAPIResult<QueryHandle>>() {}).getData();
    // wait till the query finishes
    LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request(mt)
      .get(LensQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.finished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request(mt)
        .get(LensQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    final String debugHelpMsg = "Query Handle:"+ctx.getQueryHandleString();
    assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, debugHelpMsg);
    assertTrue(ctx.getSubmissionTime() > 0, debugHelpMsg);
    assertTrue(ctx.getLaunchTime() > 0, debugHelpMsg);
    assertTrue(ctx.getDriverStartTime() > 0, debugHelpMsg);
    assertTrue(ctx.getDriverFinishTime() > 0, debugHelpMsg);
    assertTrue(ctx.getFinishTime() > 0, debugHelpMsg);
  }

  public static void createTable(String tblName, WebTarget parent, LensSessionHandle lensSessionId, MediaType mt)
    throws InterruptedException {
    createTable(tblName, parent, lensSessionId, "(ID INT, IDSTR STRING)", mt);
  }

  public static void loadData(String tblName, final String testDataFile, WebTarget parent,
      LensSessionHandle lensSessionId, MediaType mt) throws InterruptedException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    final WebTarget target = parent.path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    String dataLoad = "LOAD DATA LOCAL INPATH '" + testDataFile + "' OVERWRITE INTO TABLE " + tblName;

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
        mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), dataLoad));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
        mt));

    final QueryHandle handle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      new GenericType<LensAPIResult<QueryHandle>>() {
      }).getData();

    // wait till the query finishes
    LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request(mt)
        .get(LensQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.finished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request(mt)
        .get(LensQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }
  /**
   * Load data.
   *
   * @param tblName        the tbl name
   * @param testDataFile the test data file
   * @param parent         the parent
   * @param lensSessionId  the lens session id
   * @throws InterruptedException the interrupted exception
   */
  public static void loadDataFromClasspath(String tblName, final String testDataFile, WebTarget parent,
      LensSessionHandle lensSessionId, MediaType mt) throws InterruptedException {

    String absolutePath = LensServerTestUtil.class.getClassLoader().getResource(testDataFile).getPath();
    loadData(tblName, absolutePath, parent, lensSessionId, mt);
  }

  /**
   * Drop table.
   *
   * @param tblName       the tbl name
   * @param parent        the parent
   * @param lensSessionId the lens session id
   * @throws InterruptedException the interrupted exception
   */
  public static void dropTable(String tblName, WebTarget parent, LensSessionHandle lensSessionId, MediaType mt)
    throws InterruptedException {
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    dropTableWithConf(tblName, parent, lensSessionId, conf, mt);
  }

  /**
   * Drop table with conf passed.
   *
   * @param tblName       the tbl name
   * @param parent        the parent
   * @param lensSessionId the lens session id
   * @param conf          the query conf
   *
   * @throws InterruptedException
   */
  public static void dropTableWithConf(String tblName, WebTarget parent, LensSessionHandle lensSessionId,
    LensConf conf, MediaType mt) throws InterruptedException {
    final WebTarget target = parent.path("queryapi/queries");

    final FormDataMultiPart mp = new FormDataMultiPart();
    String createTable = "DROP TABLE IF EXISTS " + tblName;

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionId,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(), createTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(), "execute"));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("conf").fileName("conf").build(), conf,
      mt));

    final QueryHandle handle = target.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
        new GenericType<LensAPIResult<QueryHandle>>() {}).getData();

    // wait till the query finishes
    LensQuery ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request(mt)
      .get(LensQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.finished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", lensSessionId).request(mt)
        .get(LensQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  /**
   * Creates the hive table.
   *
   * @param tableName the table name
   * @throws HiveException the hive exception
   */
  public static void createHiveTable(String tableName, Map<String, String> parameters) throws HiveException {
    List<FieldSchema> columns = new ArrayList<FieldSchema>();
    columns.add(new FieldSchema("col1", "string", ""));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pcol1", "string", ""));
    Map<String, String> params = new HashMap<String, String>();
    params.put("test.hive.table.prop", "tvalue");
    if (null != parameters && !parameters.isEmpty()) {
      params.putAll(parameters);
    }
    Table tbl = Hive.get().newTable(tableName);
    tbl.setTableType(TableType.MANAGED_TABLE);
    tbl.getTTable().getSd().setCols(columns);
    tbl.setPartCols(partCols);
    tbl.getTTable().getParameters().putAll(params);
    Hive.get().createTable(tbl);
  }

  /**
   * Drop hive table.
   *
   * @param tableName the table name
   * @throws HiveException the hive exception
   */
  public static void dropHiveTable(String tableName) throws HiveException {
    Hive.get().dropTable(tableName);
  }

  public static void createTestDatabaseResources(String[] testDatabases, HiveConf conf) throws Exception {
    File srcJarDir = new File("target/testjars/");
    if (!srcJarDir.exists()) {
      // nothing to setup
      return;
    }
    File resDir = new File("target/resources");
    if (!resDir.exists()) {
      resDir.mkdir();
    }

    // Create databases and resource dirs
    Hive hive = Hive.get(conf);
    File testJarFile = new File("target/testjars/test.jar");
    File serdeJarFile = new File("target/testjars/serde.jar");
    for (String db : testDatabases) {
      Database database = new Database();
      database.setName(db);
      hive.createDatabase(database, true);
      File dbDir = new File(resDir, db);
      if (!dbDir.exists()) {
        dbDir.mkdir();
      }
      // Add a jar in the directory
      try {

        String[] jarOrder = {
          "x_" + db + ".jar",
          "y_" + db + ".jar",
          "z_" + db + ".jar",
          "serde.jar",
        };

        // Jar order is -> z, y, x, File listing order is x, y, z
        // We are explicitly specifying jar order
        FileUtils.writeLines(new File(dbDir, "jar_order"), Arrays.asList(jarOrder[2], jarOrder[1],
          jarOrder[0], jarOrder[3]));

        FileUtils.copyFile(testJarFile, new File(dbDir, jarOrder[0]));
        FileUtils.copyFile(testJarFile, new File(dbDir, jarOrder[1]));
        FileUtils.copyFile(testJarFile, new File(dbDir, jarOrder[2]));
        FileUtils.copyFile(serdeJarFile, new File(dbDir, jarOrder[3]));
      } catch (FileNotFoundException fnf) {
        log.error("File not found.", fnf);
      }
    }
  }


  public static LensSessionHandle openSession(WebTarget target, final String userName, final String passwd, final
  LensConf
    conf, MediaType
    mt) {

    final WebTarget sessionTarget = target.path("session");
    final FormDataMultiPart mp = new FormDataMultiPart();

    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("username").build(), userName));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("password").build(), passwd));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionconf").fileName("sessionconf").build(),
      conf, mt));

    return sessionTarget.request(mt).post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
      LensSessionHandle.class);

  }

  public static void addResource(WebTarget target, final LensSessionHandle lensSessionHandle, final String
    resourceType, final String resourcePath, MediaType mt) {
    final WebTarget resourceTarget = target.path("session/resources");
    final FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(), lensSessionHandle,
      mt));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("type").build(), resourceType));
    mp.bodyPart(
      new FormDataBodyPart(FormDataContentDisposition.name("path").build(), resourcePath));
    APIResult result = resourceTarget.path("add").request(mt)
      .put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);

    if (!result.getStatus().equals(APIResult.Status.SUCCEEDED)) {
      throw new RuntimeException("Could not add resource:" + result);
    }
  }

}
