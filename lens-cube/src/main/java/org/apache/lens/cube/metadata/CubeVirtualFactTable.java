/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.metadata;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;

public class CubeVirtualFactTable extends AbstractCubeTable implements FactTable {

  @Getter
  @Setter
  private FactTable sourceCubeFactTable;
  private String cubeName;
  private static final List<FieldSchema> COLUMNS = new ArrayList<FieldSchema>();
  @Getter
  private Optional<Double> virtualFactWeight = Optional.absent();

  static {
    COLUMNS.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public CubeVirtualFactTable(Table hiveTable, FactTable sourceCubeFactTable) {
    super(hiveTable);
    this.cubeName = this.getProperties().get(MetastoreUtil.getFactCubeNameKey(getName()));
    this.sourceCubeFactTable = sourceCubeFactTable;

    String wtStr = getProperties().get(MetastoreUtil.getCubeTableWeightKey(getName()));
    if (wtStr != null) {
      this.virtualFactWeight = Optional.of(Double.parseDouble(wtStr));
    }
  }

  public CubeVirtualFactTable(String cubeName, String virtualFactName, Optional<Double> weight,
    Map<String, String> properties, FactTable sourceFact) {
    super(virtualFactName, COLUMNS, properties, weight.isPresent() ? weight.get() : sourceFact.weight());
    this.cubeName = cubeName;
    this.virtualFactWeight = weight;
    this.sourceCubeFactTable = sourceFact;
    addProperties();
  }

  /**
   * Alters the weight of table
   *
   * @param weight Weight of the table.
   */
  @Override
  public void alterWeight(double weight) {
    this.virtualFactWeight = Optional.of(weight);
    this.addProperties();
  }

  @Override
  protected void addProperties() {
    getProperties().put(MetastoreConstants.TABLE_TYPE_KEY, getTableType().name());
    getProperties().put(MetastoreUtil.getSourceFactNameKey(this.getName()), this.sourceCubeFactTable.getName());
    if (virtualFactWeight.isPresent()) {
      getProperties().put(MetastoreUtil.getCubeTableWeightKey(this.getName()), String.valueOf(virtualFactWeight.get()));
    }
    this.getProperties().put(MetastoreUtil.getFactCubeNameKey(getName()), cubeName);
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.FACT;
  }

  @Override
  public Set<String> getValidColumns() {
    String validColsStr =
      MetastoreUtil.getNamedStringValue(this.getProperties(), MetastoreUtil.getValidColumnsKey(getName()));
    if (validColsStr == null) {
      return this.sourceCubeFactTable.getValidColumns();
    } else {
      return new HashSet<>(Arrays.asList(StringUtils.split(validColsStr.toLowerCase(),
        ',')));
    }
  }

  @Override
  public Set<String> getStorages() {
    return this.sourceCubeFactTable.getStorages();
  }

  @Override
  public Map<String, Set<UpdatePeriod>> getUpdatePeriods() {
    return this.sourceCubeFactTable.getUpdatePeriods();
  }

  @Override
  public String getCubeName() {
    return this.cubeName;
  }

  @Override
  public String getDataCompletenessTag() {
    return this.sourceCubeFactTable.getDataCompletenessTag();
  }

  @Override
  public boolean isAggregated() {
    return this.sourceCubeFactTable.isAggregated();
  }

  @Override
  public List<FieldSchema> getColumns() {
    return this.sourceCubeFactTable.getColumns();
  }

  @Override
  public double weight() {
    return virtualFactWeight.isPresent() ? virtualFactWeight.get() : sourceCubeFactTable.weight();
  }

  public Date getAbsoluteStartTime() {
    String absoluteStartTime = this.getProperties().get(MetastoreConstants.FACT_ABSOLUTE_START_TIME);
    Date absoluteDate = null;
    if (StringUtils.isNotBlank(absoluteStartTime)) {
      absoluteDate = MetastoreUtil.getDateFromProperty(absoluteStartTime, false, true);
    }
    return absoluteDate == null ? this.sourceCubeFactTable.getAbsoluteStartTime() : absoluteDate;
  }

  public Date getRelativeStartTime() {
    String relativeStartTime = this.getProperties().get(MetastoreConstants.FACT_RELATIVE_START_TIME);
    Date relativeDate = null;
    if (StringUtils.isNotBlank(relativeStartTime)) {
      relativeDate = MetastoreUtil.getDateFromProperty(relativeStartTime, true, true);
    }
    return relativeDate == null ? this.sourceCubeFactTable.getRelativeStartTime() : relativeDate;
  }

  public Date getStartTime() {
    return Collections.max(Lists.newArrayList(getRelativeStartTime(), getAbsoluteStartTime()));
  }

  public Date getAbsoluteEndTime() {
    String absoluteEndTime = this.getProperties().get(MetastoreConstants.FACT_ABSOLUTE_END_TIME);
    Date absoluteDate = null;
    if (StringUtils.isNotBlank(absoluteEndTime)) {
      absoluteDate = MetastoreUtil.getDateFromProperty(absoluteEndTime, false, false);
    }
    return absoluteDate == null ? this.sourceCubeFactTable.getAbsoluteEndTime() : absoluteDate;
  }

  public Date getRelativeEndTime() {
    String relativeEndTime = this.getProperties().get(MetastoreConstants.FACT_RELATIVE_END_TIME);
    Date relativeDate = null;
    if (StringUtils.isNotBlank(relativeEndTime)) {
      relativeDate = MetastoreUtil.getDateFromProperty(relativeEndTime, true, false);
    }
    return relativeDate == null ? this.sourceCubeFactTable.getRelativeEndTime() : relativeDate;
  }

  public Date getEndTime() {
    return Collections.min(Lists.newArrayList(getRelativeEndTime(), getAbsoluteEndTime()));
  }

  @Override
  public boolean isVirtualFact() {
    return true;
  }

  @Override
  public String getSourceFactName() {
    return this.sourceCubeFactTable.getName();
  }

  @Override
  public Map<String, String> getSourceFactProperties() {
    return getSourceCubeFactTable().getProperties();
  }

  @Override
  public Set<String> getPartitionColumns(String storage) {
    return new HashSet<String>();
  }
}
