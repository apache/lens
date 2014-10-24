package org.apache.lens.storage.db;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.StoragePartitionDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * The Class DBStorage.
 */
public class DBStorage extends Storage {

  /** The Constant DB_URL. */
  public static final String DB_URL = "lens.storage.db.url";

  /** The db url. */
  private String dbUrl = null;

  /**
   * Instantiates a new DB storage.
   *
   * @param dbUrl
   *          the db url
   * @param name
   *          the name
   * @param properties
   *          the properties
   */
  protected DBStorage(String dbUrl, String name, Map<String, String> properties) {
    super(name, properties);
    this.dbUrl = dbUrl;
    addProperties();
  }

  /**
   * Instantiates a new DB storage.
   *
   * @param name
   *          the name
   */
  public DBStorage(String name) {
    this(name, null);
  }

  /**
   * Instantiates a new DB storage.
   *
   * @param name
   *          the name
   * @param properties
   *          the properties
   */
  protected DBStorage(String name, Map<String, String> properties) {
    super(name, properties);
    if (properties != null) {
      this.dbUrl = properties.get(DB_URL);
    }
  }

  /**
   * Instantiates a new DB storage.
   *
   * @param hiveTable
   *          the hive table
   */
  public DBStorage(Table hiveTable) {
    super(hiveTable);
    this.dbUrl = getProperties().get(DB_URL);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.ql.cube.metadata.Storage#addProperties()
   */
  protected void addProperties() {
    super.addProperties();
    if (dbUrl != null) {
      getProperties().put(DB_URL, dbUrl);
    }
  }

  public String getDbUrl() {
    return dbUrl;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.cube.metadata.PartitionMetahook#commitAddPartition(org.apache.hadoop.hive.ql.cube.metadata
   * .StoragePartitionDesc)
   */
  @Override
  public void commitAddPartition(StoragePartitionDesc arg0) throws HiveException {

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.ql.cube.metadata.PartitionMetahook#commitDropPartition(java.lang.String,
   * java.util.List)
   */
  @Override
  public void commitDropPartition(String arg0, List<String> arg1) throws HiveException {

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.cube.metadata.PartitionMetahook#preAddPartition(org.apache.hadoop.hive.ql.cube.metadata
   * .StoragePartitionDesc)
   */
  @Override
  public void preAddPartition(StoragePartitionDesc arg0) throws HiveException {

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.ql.cube.metadata.PartitionMetahook#preDropPartition(java.lang.String, java.util.List)
   */
  @Override
  public void preDropPartition(String arg0, List<String> arg1) throws HiveException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.cube.metadata.PartitionMetahook#rollbackAddPartition(org.apache.hadoop.hive.ql.cube.metadata
   * .StoragePartitionDesc)
   */
  @Override
  public void rollbackAddPartition(StoragePartitionDesc arg0) throws HiveException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.ql.cube.metadata.PartitionMetahook#rollbackDropPartition(java.lang.String,
   * java.util.List)
   */
  @Override
  public void rollbackDropPartition(String arg0, List<String> arg1) throws HiveException {
  }
}
