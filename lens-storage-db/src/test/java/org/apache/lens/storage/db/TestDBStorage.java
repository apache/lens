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
package org.apache.lens.storage.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lens.cube.metadata.*;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestDBStorage.
 */
public class TestDBStorage {

  /**
   * The D b_ storag e1.
   */
  public static final String DB_STORAGE1 = "db1";

  /**
   * The D b_ storag e2.
   */
  public static final String DB_STORAGE2 = "db2";

  /**
   * The conf.
   */
  HiveConf conf = new HiveConf(this.getClass());

  /**
   * The db1.
   */
  Storage db1;

  /**
   * The db2.
   */
  Storage db2;

  TestDBStorage() throws Exception {
    db1 = new DBStorage(DB_STORAGE1, DB_STORAGE1, null);
    db2 = new DBStorage(DB_STORAGE2, DB_STORAGE2, null);

  }
  /**
   * Setup.
   *
   * @throws AlreadyExistsException the already exists exception
   * @throws HiveException          the hive exception
   * @throws IOException            Signals that an I/O exception has occurred.
   */
  @BeforeTest
  public void setup() throws AlreadyExistsException, HiveException, IOException {
    SessionState.start(conf);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestDBStorage.class.getSimpleName());
    client.createDatabase(database);
    SessionState.get().setCurrentDatabase(TestDBStorage.class.getSimpleName());
  }

  /**
   * Tear down.
   *
   * @throws HiveException         the hive exception
   * @throws NoSuchObjectException the no such object exception
   */
  @AfterTest
  public void tearDown() throws HiveException, NoSuchObjectException {
    Hive client = Hive.get(conf);
    client.dropDatabase(TestDBStorage.class.getSimpleName(), true, true, true);
  }

  /**
   * Test db storage.
   *
   * @throws HiveException the hive exception
   */
  @Test(groups = "first")
  public void testDBStorage() throws Exception {
    CubeMetastoreClient cc = CubeMetastoreClient.getInstance(conf);
    if (!cc.tableExists(DB_STORAGE1)) {
      cc.createStorage(db1);
    }
    if (!cc.tableExists(DB_STORAGE2)) {
      cc.createStorage(db2);
    }
  }

  /**
   * Test cube dim.
   *
   * @throws Exception the exception
   */
  @Test(dependsOnGroups = "first")
  public void testCubeDim() throws Exception {
    CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);
    String dimTblName = "ziptableMeta";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));
    dimColumns.add(new FieldSchema("statei2", "int", "state id"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setStorageHandler(DBStorageHandler.class.getCanonicalName());
    s1.setExternal(true);
    dumpPeriods.put(db1.getName(), null);
    dumpPeriods.put(db2.getName(), null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(db1.getName(), s1);
    storageTables.put(db2.getName(), s1);
    client.createCubeDimensionTable("zipdim", dimTblName, dimColumns, 0L, dumpPeriods, null, storageTables);

    Assert.assertTrue(client.tableExists(dimTblName));

    // Assert for storage tables
    for (String storage : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactOrDimtableStorageTableName(dimTblName, storage);
      Assert.assertTrue(client.tableExists(storageTableName));
    }
  }

}
