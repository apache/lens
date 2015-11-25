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

package org.apache.lens.cube.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;

public class StorageTableDesc extends CreateTableDesc {
  private static final long serialVersionUID = 1L;

  private List<String> timePartCols;

  /**
   * @return the timePartCols
   */
  public List<String> getTimePartCols() {
    return timePartCols;
  }

  public void setTimePartCols(List<String> timePartCols) {
    this.timePartCols = timePartCols;
    if (super.getTblProps() == null) {
      super.setTblProps(new HashMap<String, String>());
    }
    super.getTblProps().put(MetastoreConstants.TIME_PART_COLUMNS, StringUtils.join(this.timePartCols, ','));
  }

  public StorageTableDesc() {
  }

  public StorageTableDesc(Class<?> inputFormatClass, Class<?> outputFormatClass,
    ArrayList<FieldSchema> partCols, List<String> timePartCols) {
    if (inputFormatClass != null) {
      setInputFormat(inputFormatClass.getCanonicalName());
    }
    if (outputFormatClass != null) {
      setOutputFormat(outputFormatClass.getCanonicalName());
    }
    if (partCols != null) {
      setPartCols(partCols);
    }
    if (timePartCols != null) {
      setTimePartCols(timePartCols);
    }
  }

  /**
   * @deprecated
   */
  @Override
  @Deprecated
  public String getTableName() {
    return super.getTableName();
  }

  /**
   * This is not honored.
   *
   * @deprecated
   */
  @Override
  @Deprecated
  public void setTableName(String tableName) {
  }

}
