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
package org.apache.lens.cube.metadata.timeline;


import java.util.*;

import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Represents the in-memory data structure that represents timeline of all existing partitions for a given storage
 * table, update period, partition column. Is an Abstract class. Can be implemented in multiple ways.
 *
 * @see org.apache.lens.cube.metadata.timeline.EndsAndHolesPartitionTimeline
 * @see org.apache.lens.cube.metadata.timeline.StoreAllPartitionTimeline
 */
@Data
@ToString(exclude = {"client"})
@CommonsLog
public abstract class PartitionTimeline implements Iterable<TimePartition> {
  private final String storageTableName;
  private final UpdatePeriod updatePeriod;
  private final String partCol;
  private TreeSet<TimePartition> all;

  /** wrapper on latest data */
  public Date getLatestDate() {
    return latest() == null ? null : latest().getDate();
  }

  /**
   * Sets PartitionTimeline implementation class's name and specific params in table param.
   *
   * @param table
   * @see #init(org.apache.hadoop.hive.ql.metadata.Table)
   */
  public void updateTableParams(Table table) {
    String prefix = MetastoreUtil.getPartitionInfoKeyPrefix(getUpdatePeriod(), getPartCol());
    String storageClass = MetastoreUtil.getPartitionTimelineStorageClassKey(getUpdatePeriod(), getPartCol());
    table.getParameters().put(storageClass, this.getClass().getCanonicalName());
    for (Map.Entry<String, String> entry : toProperties().entrySet()) {
      table.getParameters().put(prefix + entry
        .getKey(), entry.getValue());
    }
  }

  /**
   * Extracts timeline implementation class from table params and instantiates it with other arguments, also in table
   * params.
   *
   * @param table
   * @throws LensException
   * @see #updateTableParams(org.apache.hadoop.hive.ql.metadata.Table)
   */
  public void init(Table table) throws LensException {
    HashMap<String, String> props = Maps.newHashMap();
    String prefix = MetastoreUtil.getPartitionInfoKeyPrefix(getUpdatePeriod(), getPartCol());
    for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        props.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }
    log.info("initializing timeline: " + getStorageTableName() + ", " + getUpdatePeriod() + ", " + getPartCol());
    initFromProperties(props);
    log.info("initialized to " + toProperties());
  }

  /**
   * Add partition to local memory to be sent for batch addition.
   *
   * @see #commitBatchAdditions()
   */
  public void addForBatchAddition(TimePartition partition) {
    if (all == null) {
      all = Sets.newTreeSet();
    }
    all.add(partition);
  }

  /**
   * Commit all partitions that were added to batch addition queue. //TODO: improve batch addition implementation.
   *
   * @return true if all the partitions were added successfully, or no partitions needed to be added
   * @throws LensException
   */
  public boolean commitBatchAdditions() throws LensException {
    if (getAll() == null) {
      return true;
    }
    boolean result = add(getAll());
    all = null;
    log.info("after commit batch additions, timeline is: " + this);
    return result;
  }


  /**
   * Add partition to timeline
   *
   * @param partition
   * @return whether add was successful
   * @throws LensException
   */
  public abstract boolean add(@NonNull TimePartition partition) throws LensException;

  /**
   * Add multiple partitions to timeline
   *
   * @param partitions
   * @return whether add was successful
   * @throws LensException
   */
  public boolean add(@NonNull Collection<TimePartition> partitions) throws LensException {
    boolean result = true;
    for (TimePartition partition : partitions) {
      result &= add(partition);
    }
    // Can also return the failed to add items.
    return result;
  }

  /**
   * drop partition.
   *
   * @param toDrop
   * @return whether drop was successful
   * @throws LensException
   */
  public abstract boolean drop(@NonNull TimePartition toDrop) throws LensException;

  /**
   * latest partition. will be null if no partitions exist.
   *
   * @return
   */
  public abstract TimePartition latest();

  /**
   * serialize member objects as map
   *
   * @return
   */
  public abstract Map<String, String> toProperties();

  /**
   * deserialize member variables from given map
   *
   * @param properties
   * @return true if after deserializing, the timeline is in consistent state
   * @throws LensException
   * @see #isConsistent()
   */
  public abstract boolean initFromProperties(Map<String, String> properties) throws LensException;

  /**
   * Whether No partitions have been registered
   *
   * @return
   */
  public abstract boolean isEmpty();

  /**
   * whether timeline is in consistent state
   *
   * @return
   */
  public abstract boolean isConsistent();

  /**
   * Checks partition existance
   *
   * @param partition
   * @return
   */
  public abstract boolean exists(TimePartition partition);
}
