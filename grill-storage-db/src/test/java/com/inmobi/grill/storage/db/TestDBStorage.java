package com.inmobi.grill.storage.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.StorageTableDesc;
import org.apache.hadoop.hive.ql.cube.metadata.TableReference;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestDBStorage {

  public static String DB_STORAGE1 = "db1";
  public static String DB_STORAGE2 = "db2";
  HiveConf conf = new HiveConf(this.getClass());
  Storage db1 = new DBStorage(DB_STORAGE1, DB_STORAGE1, null);
  Storage db2 = new DBStorage(DB_STORAGE2, DB_STORAGE2, null);

  @BeforeTest
  public void setup()
      throws AlreadyExistsException, HiveException, IOException {
    SessionState.start(conf);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestDBStorage.class.getSimpleName());
    client.createDatabase(database);
    SessionState.get().setCurrentDatabase(TestDBStorage.class.getSimpleName());
  }

  @AfterTest
  public void tearDown() throws HiveException, NoSuchObjectException {
    Hive client = Hive.get(conf);
    client.dropDatabase(TestDBStorage.class.getSimpleName(), true, true,
        true);
  }

  @Test(groups = "first")
  public void testDBStorage() throws HiveException {
    CubeMetastoreClient cc = CubeMetastoreClient.getInstance(conf);
    if (!cc.tableExists(DB_STORAGE1)) {
      cc.createStorage(db1);
    }
    if (!cc.tableExists(DB_STORAGE2)) {
      cc.createStorage(db2);
    }
  }

  @Test(dependsOnGroups = "first")
  public void testCubeDim() throws Exception {
    CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);
    String dimName = "ziptableMeta";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));
    dimColumns.add(new FieldSchema("statei2", "int", "state id"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();


    dimensionReferences.put("stateid", Arrays.asList(new TableReference("statetable", "id")));

    final TableReference stateRef = new TableReference("statetable", "id");
    final TableReference cityRef =  new TableReference("citytable", "id");
    dimensionReferences.put("stateid2",
        Arrays.asList(stateRef, cityRef));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setStorageHandler(DBStorageHandler.class.getCanonicalName());
    s1.setExternal(true);
    dumpPeriods.put(db1.getName(), null);
    dumpPeriods.put(db2.getName(), null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(db1.getName(), s1);
    storageTables.put(db2.getName(), s1);
    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, dumpPeriods, null, storageTables);

    Assert.assertTrue(client.tableExists(dimName));

    // Assert for storage tables
    for (String storage : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          storage);
      Assert.assertTrue(client.tableExists(storageTableName));
    }
  }

  
}
