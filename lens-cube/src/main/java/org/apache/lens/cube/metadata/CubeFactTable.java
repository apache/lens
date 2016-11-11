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

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.UpdatePeriod.UpdatePeriodComparator;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CubeFactTable extends AbstractCubeTable {
  private String cubeName;
  private final Map<String, Set<UpdatePeriod>> storageUpdatePeriods;

  public CubeFactTable(Table hiveTable) {
    super(hiveTable);
    this.storageUpdatePeriods = getUpdatePeriods(getName(), getProperties());
    this.cubeName = getCubeName(getName(), getProperties());
  }

  public CubeFactTable(String cubeName, String factName, List<FieldSchema> columns,
    Map<String, Set<UpdatePeriod>> storageUpdatePeriods) {
    this(cubeName, factName, columns, storageUpdatePeriods, 0L, new HashMap<String, String>());
  }

  public CubeFactTable(String cubeName, String factName, List<FieldSchema> columns,
    Map<String, Set<UpdatePeriod>> storageUpdatePeriods, double weight) {
    this(cubeName, factName, columns, storageUpdatePeriods, weight, new HashMap<String, String>());
  }

  public CubeFactTable(String cubeName, String factName, List<FieldSchema> columns,
    Map<String, Set<UpdatePeriod>> storageUpdatePeriods, double weight, Map<String, String> properties) {
    super(factName, columns, properties, weight);
    this.cubeName = cubeName;
    this.storageUpdatePeriods = storageUpdatePeriods;
    addProperties();
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    addCubeNames(getName(), getProperties(), cubeName);
    addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
  }

  private static void addUpdatePeriodProperies(String name, Map<String, String> props,
    Map<String, Set<UpdatePeriod>> updatePeriods) {
    if (updatePeriods != null) {
      props.put(MetastoreUtil.getFactStorageListKey(name), MetastoreUtil.getStr(updatePeriods.keySet()));
      for (Map.Entry<String, Set<UpdatePeriod>> entry : updatePeriods.entrySet()) {
        props.put(MetastoreUtil.getFactUpdatePeriodKey(name, entry.getKey()),
          MetastoreUtil.getNamedStr(entry.getValue()));
      }
    }
  }

  private static void addCubeNames(String factName, Map<String, String> props, String cubeName) {
    props.put(MetastoreUtil.getFactCubeNameKey(factName), cubeName);
  }

  private static Map<String, Set<UpdatePeriod>> getUpdatePeriods(String name, Map<String, String> props) {
    Map<String, Set<UpdatePeriod>> storageUpdatePeriods = new HashMap<>();
    String storagesStr = props.get(MetastoreUtil.getFactStorageListKey(name));
    if (!StringUtils.isBlank(storagesStr)) {
      String[] storages = storagesStr.split(",");
      for (String storage : storages) {
        String updatePeriodStr = props.get(MetastoreUtil.getFactUpdatePeriodKey(name, storage));
        if (StringUtils.isNotBlank(updatePeriodStr)) {
          String[] periods = updatePeriodStr.split(",");
          Set<UpdatePeriod> updatePeriods = new TreeSet<>();
          for (String period : periods) {
            updatePeriods.add(UpdatePeriod.valueOf(period));
          }
          storageUpdatePeriods.put(storage, updatePeriods);
        }
      }
    }
    return storageUpdatePeriods;
  }

  static String getCubeName(String factName, Map<String, String> props) {
    return props.get(MetastoreUtil.getFactCubeNameKey(factName));
  }

  public Map<String, Set<UpdatePeriod>> getUpdatePeriods() {
    return storageUpdatePeriods;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }

    CubeFactTable other = (CubeFactTable) obj;
    if (!this.getCubeName().equals(other.getCubeName())) {
      return false;
    }
    if (this.getUpdatePeriods() == null) {
      if (other.getUpdatePeriods() != null) {
        return false;
      }
    } else {
      if (!this.getUpdatePeriods().equals(other.getUpdatePeriods())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.FACT;
  }

  /**
   * Get partition value strings for given range, for the specified updateInterval
   *
   * @param fromDate
   * @param toDate
   * @param interval
   * @return
   */
  public List<String> getPartitions(Date fromDate, Date toDate, UpdatePeriod interval) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(fromDate);
    List<String> partitions = new ArrayList<String>();
    Date dt = cal.getTime();
    while (dt.compareTo(toDate) < 0) {
      String part = interval.format(cal.getTime());
      partitions.add(part);
      cal.add(interval.calendarField(), 1);
      dt = cal.getTime();
    }
    return partitions;
  }

  /**
   * Get the max update period for the given range and available update periods
   *
   * @param from
   * @param to
   * @param updatePeriods
   * @return
   */
  public static UpdatePeriod maxIntervalInRange(Date from, Date to, Set<UpdatePeriod> updatePeriods) {
    UpdatePeriod max = null;

    long diff = to.getTime() - from.getTime();
    if (diff < UpdatePeriod.MIN_INTERVAL) {
      return null;
    }

    // Use weight only till UpdatePeriod.DAILY
    // Above Daily, check if at least one full update period is present
    // between the dates
    UpdatePeriodComparator cmp = new UpdatePeriodComparator();
    for (UpdatePeriod i : updatePeriods) {
      if (UpdatePeriod.YEARLY == i || UpdatePeriod.QUARTERLY == i || UpdatePeriod.MONTHLY == i
        || UpdatePeriod.WEEKLY == i) {
        int intervals = DateUtil.getTimeDiff(from, to, i);
        if (intervals > 0) {
          if (cmp.compare(i, max) > 0) {
            max = i;
          }
        }
      } else {
        // Below WEEKLY, we can use weight to find out the correct period
        if (diff < i.weight()) {
          // interval larger than time diff
          continue;
        }

        if (cmp.compare(i, max) > 0) {
          max = i;
        }
      }
    }
    return max;
  }

  @Override
  public Set<String> getStorages() {
    return storageUpdatePeriods.keySet();
  }

  public String getCubeName() {
    return cubeName;
  }

  /**
   * Return valid columns of the fact, which can be specified by property MetastoreUtil.getValidColumnsKey(getName())
   *
   * @return
   */
  public List<String> getValidColumns() {
    String validColsStr =
      MetastoreUtil.getNamedStringValue(getProperties(), MetastoreUtil.getValidColumnsKey(getName()));
    return validColsStr == null ? null : Arrays.asList(StringUtils.split(validColsStr.toLowerCase(), ','));
  }

  /**
   * Add update period to storage
   *
   * @param storage
   * @param period
   */
  public void addUpdatePeriod(String storage, UpdatePeriod period) {
    if (storageUpdatePeriods.containsKey(storage)) {
      storageUpdatePeriods.get(storage).add(period);
    } else {
      storageUpdatePeriods.put(storage, new HashSet<UpdatePeriod>(Arrays.asList(period)));
    }
    addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
  }

  /**
   * Remove update period from storage
   *
   * @param storage
   * @param period
   */
  public void removeUpdatePeriod(String storage, UpdatePeriod period) {
    if (storageUpdatePeriods.containsKey(storage)) {
      storageUpdatePeriods.get(storage).remove(period);
      addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
    }
  }

  /**
   * Alter a storage with specified update periods
   *
   * @param storage
   * @param updatePeriods
   */
  public void alterStorage(String storage, Set<UpdatePeriod> updatePeriods) throws LensException{
    if (!storageUpdatePeriods.containsKey(storage)) {
      throw new LensException(LensCubeErrorCode.ERROR_IN_ENTITY_DEFINITION.getLensErrorInfo(),
        "Invalid storage" + storage);
    }
    storageUpdatePeriods.put(storage, updatePeriods);
    addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
  }

  /**
   * Add a storage with specified update periods
   *
   * @param storage
   * @param updatePeriods
   */
  void addStorage(String storage, Set<UpdatePeriod> updatePeriods) {
    storageUpdatePeriods.put(storage, updatePeriods);
    addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
  }

  /**
   * Drop a storage from the fact
   *
   * @param storage
   */
  void dropStorage(String storage) {
    storageUpdatePeriods.remove(storage);
    getProperties().remove(MetastoreUtil.getFactUpdatePeriodKey(getName(), storage));
    String newStorages = StringUtils.join(storageUpdatePeriods.keySet(), ",");
    getProperties().put(MetastoreUtil.getFactStorageListKey(getName()), newStorages);
  }

  @Override
  public void alterColumn(FieldSchema column) {
    super.alterColumn(column);
  }

  @Override
  public void addColumns(Collection<FieldSchema> columns) {
    super.addColumns(columns);
  }

  /**
   * Alter the cubeName to which this fact belongs
   *
   * @param cubeName
   */
  public void alterCubeName(String cubeName) {
    this.cubeName = cubeName;
    addCubeNames(getName(), getProperties(), cubeName);
  }

  public String getDataCompletenessTag() {
    return getProperties().get(MetastoreConstants.FACT_DATA_COMPLETENESS_TAG);
  }

  public boolean isAggregated() {
    // It's aggregate table unless explicitly set to false
    return !"false".equalsIgnoreCase(getProperties().get(MetastoreConstants.FACT_AGGREGATED_PROPERTY));
  }

  public void setAggregated(boolean isAggregated) {
    getProperties().put(MetastoreConstants.FACT_AGGREGATED_PROPERTY, Boolean.toString(isAggregated));
  }

  public Date getAbsoluteStartTime() {
    return getDateFromProperty(MetastoreConstants.FACT_ABSOLUTE_START_TIME, false, true);
  }

  public Date getRelativeStartTime() {
    return getDateFromProperty(MetastoreConstants.FACT_RELATIVE_START_TIME, true, true);
  }

  public Date getStartTime() {
    return Collections.max(Lists.newArrayList(getRelativeStartTime(), getAbsoluteStartTime()));
  }

  public Date getAbsoluteEndTime() {
    return getDateFromProperty(MetastoreConstants.FACT_ABSOLUTE_END_TIME, false, false);
  }

  public Date getRelativeEndTime() {
    return getDateFromProperty(MetastoreConstants.FACT_RELATIVE_END_TIME, true, false);
  }

  public Date getEndTime() {
    return Collections.min(Lists.newArrayList(getRelativeEndTime(), getAbsoluteEndTime()));
  }


}
