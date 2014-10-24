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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * The Class DBStorageHandler.
 */
@SuppressWarnings("deprecation")
public class DBStorageHandler implements HiveStorageHandler, HiveMetaHook {

  /** The conf. */
  private Configuration conf;

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.metadata.HiveStorageHandler#configureInputJobProperties(org.apache.hadoop.hive.ql.plan
   * .TableDesc, java.util.Map)
   */
  @Override
  public void configureInputJobProperties(TableDesc arg0, Map<String, String> arg1) {
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.metadata.HiveStorageHandler#configureJobConf(org.apache.hadoop.hive.ql.plan.TableDesc,
   * org.apache.hadoop.mapred.JobConf)
   */
  @Override
  public void configureJobConf(TableDesc arg0, JobConf arg1) {
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.metadata.HiveStorageHandler#configureOutputJobProperties(org.apache.hadoop.hive.ql.plan
   * .TableDesc, java.util.Map)
   */
  @Override
  public void configureOutputJobProperties(TableDesc arg0, Map<String, String> arg1) {
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.metadata.HiveStorageHandler#configureTableJobProperties(org.apache.hadoop.hive.ql.plan
   * .TableDesc, java.util.Map)
   */
  @Override
  public void configureTableJobProperties(TableDesc arg0, Map<String, String> arg1) {
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return null;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return null;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return DBSerde.class;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.metastore.HiveMetaHook#commitCreateTable(org.apache.hadoop.hive.metastore.api.Table)
   */
  @Override
  public void commitCreateTable(Table arg0) throws MetaException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.metastore.HiveMetaHook#commitDropTable(org.apache.hadoop.hive.metastore.api.Table,
   * boolean)
   */
  @Override
  public void commitDropTable(Table arg0, boolean arg1) throws MetaException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.metastore.HiveMetaHook#preCreateTable(org.apache.hadoop.hive.metastore.api.Table)
   */
  @Override
  public void preCreateTable(Table arg0) throws MetaException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.metastore.HiveMetaHook#preDropTable(org.apache.hadoop.hive.metastore.api.Table)
   */
  @Override
  public void preDropTable(Table arg0) throws MetaException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.metastore.HiveMetaHook#rollbackCreateTable(org.apache.hadoop.hive.metastore.api.Table)
   */
  @Override
  public void rollbackCreateTable(Table arg0) throws MetaException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.metastore.HiveMetaHook#rollbackDropTable(org.apache.hadoop.hive.metastore.api.Table)
   */
  @Override
  public void rollbackDropTable(Table arg0) throws MetaException {
  }

}
