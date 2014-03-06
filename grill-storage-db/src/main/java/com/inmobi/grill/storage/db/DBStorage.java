package com.inmobi.grill.storage.db;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.StoragePartitionDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class DBStorage extends Storage {

  public static final String DB_URL ="grill.storage.db.url";
  private final String dbUrl;
  protected DBStorage(String dbUrl, String name, Map<String, String> properties) {
    super(name, properties);
    this.dbUrl = dbUrl;
    addProperties();
  }

  protected DBStorage(String name, Map<String, String> properties) {
    super(name, properties);
    this.dbUrl = properties.get(DB_URL);
  }

  public DBStorage(Table hiveTable) {
    super(hiveTable);
    this.dbUrl = getProperties().get(DB_URL);
  }

  protected void addProperties() {
    super.addProperties();
    getProperties().put(DB_URL, dbUrl);
  }

  public String getDbUrl() {
    return dbUrl;
  }

  @Override
  public void commitAddPartition(StoragePartitionDesc arg0)
      throws HiveException {

  }

  @Override
  public void commitDropPartition(String arg0, List<String> arg1)
      throws HiveException {

  }

  @Override
  public void preAddPartition(StoragePartitionDesc arg0) throws HiveException {

  }

  @Override
  public void preDropPartition(String arg0, List<String> arg1)
      throws HiveException {
  }

  @Override
  public void rollbackAddPartition(StoragePartitionDesc arg0)
      throws HiveException {
  }

  @Override
  public void rollbackDropPartition(String arg0, List<String> arg1)
      throws HiveException {
  }
}
