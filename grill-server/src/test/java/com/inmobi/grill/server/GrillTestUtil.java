package com.inmobi.grill.server;

import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.testng.Assert;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.api.GrillConfConstants;

/*
 * #%L
 * Grill Server
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

public class GrillTestUtil {

  public static void createTable(String tblName, WebTarget parent,
      GrillSessionHandle grillSessionId) throws InterruptedException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    final WebTarget target = parent.path("queryapi/queries");
  
    final FormDataMultiPart mp = new FormDataMultiPart();
    String createTable = "CREATE TABLE IF NOT EXISTS " + tblName  +"(ID INT, IDSTR STRING)";
  
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        createTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
  
    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
  
    // wait till the query finishes
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    assertTrue(ctx.getSubmissionTime() > 0);
    assertTrue(ctx.getLaunchTime() > 0);
    assertTrue(ctx.getDriverStartTime() > 0);
    assertTrue(ctx.getDriverFinishTime() > 0);
    assertTrue(ctx.getFinishTime() > 0);
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  public static void loadData(String tblName, final String TEST_DATA_FILE,
      WebTarget parent, GrillSessionHandle grillSessionId) throws InterruptedException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    final WebTarget target = parent.path("queryapi/queries");
  
    final FormDataMultiPart mp = new FormDataMultiPart();
    String dataLoad = "LOAD DATA LOCAL INPATH '"+ TEST_DATA_FILE +
        "' OVERWRITE INTO TABLE " + tblName;
  
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        dataLoad));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
  
    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
  
    // wait till the query finishes
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  
  }

  public static void dropTable(String tblName, WebTarget parent,
      GrillSessionHandle grillSessionId) throws InterruptedException {
    GrillConf conf = new GrillConf();
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    final WebTarget target = parent.path("queryapi/queries");
  
    final FormDataMultiPart mp = new FormDataMultiPart();
    String createTable = "DROP TABLE IF EXISTS " + tblName ;
  
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        createTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("operation").build(),
        "execute"));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        conf,
        MediaType.APPLICATION_XML_TYPE));
  
    final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);
  
    // wait till the query finishes
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid", grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }
    Assert.assertEquals(ctx.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
  }

  public static void createHiveTable(String tableName) throws HiveException {
    List<FieldSchema> columns = new ArrayList<FieldSchema>();
    columns.add(new FieldSchema("col1", "string", ""));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pcol1", "string", ""));
    Map<String, String> params = new HashMap<String, String>();
    params.put("test.hive.table.prop", "tvalue");
    Table tbl = Hive.get().newTable(tableName);
    tbl.setTableType(TableType.MANAGED_TABLE);
    tbl.getTTable().getSd().setCols(columns);
    tbl.setPartCols(partCols);
    tbl.getTTable().getParameters().putAll(params);
    Hive.get().createTable(tbl);
  }

  public static void dropHiveTable(String tableName) throws HiveException {
    Hive.get().dropTable(tableName);
  }

}
