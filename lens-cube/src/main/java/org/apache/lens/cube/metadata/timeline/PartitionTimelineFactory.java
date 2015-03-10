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


import java.lang.reflect.Constructor;

import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.UpdatePeriod;

/** factory class for getting new timeline instances */
public final class PartitionTimelineFactory {
  private PartitionTimelineFactory() {

  }

  /**
   * Checks in table params if desired implementing class is given. Otherwise by default returns instance of {@link
   * org.apache.lens.cube.metadata.timeline.EndsAndHolesPartitionTimeline}.
   *
   * @param client
   * @param storageTable
   * @param updatePeriod
   * @param partitionColumn
   * @return
   */
  public static PartitionTimeline get(CubeMetastoreClient client, String storageTable,
    UpdatePeriod updatePeriod, String partitionColumn) {
    try {
      String storageClassName = client.getTable(storageTable).getParameters().get(
        MetastoreUtil.getPartitionTimelineStorageClassKey(
          updatePeriod, partitionColumn));
      Class<? extends PartitionTimeline> clz = (Class<? extends PartitionTimeline>) Class.forName(storageClassName);
      Constructor<? extends PartitionTimeline> constructor = clz.getConstructor(
        CubeMetastoreClient.class, String.class, UpdatePeriod.class, String.class);
      return constructor.newInstance(
        client, storageTable, updatePeriod, partitionColumn);
    } catch (Exception e) {
      return new EndsAndHolesPartitionTimeline(client, storageTable, updatePeriod, partitionColumn);
    }
  }
}
