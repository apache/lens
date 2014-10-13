package org.apache.lens.storage.db;

/*
 * #%L
 * Grill DB storage
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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.StoragePartitionDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class DBStorage extends Storage {

  public static final String DB_URL ="grill.storage.db.url";
  private String dbUrl = null;
  protected DBStorage(String dbUrl, String name, Map<String, String> properties) {
    super(name, properties);
    this.dbUrl = dbUrl;
    addProperties();
  }

  public DBStorage(String name) {
    this(name, null);
  }

  protected DBStorage(String name, Map<String, String> properties) {
    super(name, properties);
    if (properties != null) {
      this.dbUrl = properties.get(DB_URL);
    }
  }

  public DBStorage(Table hiveTable) {
    super(hiveTable);
    this.dbUrl = getProperties().get(DB_URL);
  }

  protected void addProperties() {
    super.addProperties();
    if (dbUrl != null) {
      getProperties().put(DB_URL, dbUrl);
    }
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
