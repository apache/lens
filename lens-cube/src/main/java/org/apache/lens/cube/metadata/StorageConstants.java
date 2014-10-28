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
import java.util.Map;

public class StorageConstants {
  public static final String DATE_PARTITION_KEY = "dt";
  public static final String STORGAE_SEPARATOR = "_";
  public static final String LATEST_PARTITION_VALUE = "latest";

  /**
   * Get the partition spec for latest partition
   * 
   * @param The
   *          partition column for latest spec
   * 
   * @return latest partition spec as Map from String to String
   */
  public static String getLatestPartFilter(String partCol) {
    return partCol + "='" + LATEST_PARTITION_VALUE + "'";
  }

  /**
   * Get the latest partition value as List
   * 
   * @return List
   */
  public static List<String> getPartitionsForLatest() {
    List<String> parts = new ArrayList<String>();
    parts.add(LATEST_PARTITION_VALUE);
    return parts;
  }

  /**
   * Get the partition spec for latest partition
   * 
   * @param The
   *          partition column for latest spec
   * 
   * @return latest partition spec as Map from String to String
   */
  public static Map<String, String> getLatestPartSpec(Map<String, String> partSpec, String partCol) {
    Map<String, String> latestSpec = new HashMap<String, String>();
    latestSpec.putAll(partSpec);
    latestSpec.put(partCol, LATEST_PARTITION_VALUE);
    return latestSpec;
  }
}
