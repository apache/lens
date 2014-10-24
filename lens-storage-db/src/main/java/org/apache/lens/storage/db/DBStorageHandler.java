package org.apache.lens.storage.db;

/*
 * #%L
 * Lens DB storage
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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

@SuppressWarnings("deprecation")
public class DBStorageHandler implements HiveStorageHandler, HiveMetaHook {

  private Configuration conf;

  @Override
  public void configureInputJobProperties(TableDesc arg0,
      Map<String, String> arg1) {
  }

  @Override
  public void configureJobConf(TableDesc arg0, JobConf arg1) {
  }

  @Override
  public void configureOutputJobProperties(TableDesc arg0,
      Map<String, String> arg1) {
  }

  @Override
  public void configureTableJobProperties(TableDesc arg0,
      Map<String, String> arg1) {
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider()
      throws HiveException {
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

  @Override
  public void commitCreateTable(Table arg0) throws MetaException {
  }

  @Override
  public void commitDropTable(Table arg0, boolean arg1) throws MetaException {
  }

  @Override
  public void preCreateTable(Table arg0) throws MetaException {
  }

  @Override
  public void preDropTable(Table arg0) throws MetaException {
  }

  @Override
  public void rollbackCreateTable(Table arg0) throws MetaException {
  }

  @Override
  public void rollbackDropTable(Table arg0) throws MetaException {
  }

}
