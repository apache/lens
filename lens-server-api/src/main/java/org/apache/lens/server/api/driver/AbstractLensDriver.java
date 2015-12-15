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
package org.apache.lens.server.api.driver;


import org.apache.lens.api.Priority;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import lombok.Getter;

/**
 * Abstract class for Lens Driver Implementations. Provides default
 * implementations and some utility methods for drivers
 */
public abstract class AbstractLensDriver implements LensDriver {
  /**
   * Separator used for constructing fully qualified name and driver resource path
   */
  private static final char SEPARATOR = '/';

  /**
   * Driver's fully qualified name ( Example hive/hive1, jdbc/mysql1)
   */
  @Getter
  private String fullyQualifiedName = null;

  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    if (StringUtils.isBlank(driverType) || StringUtils.isBlank(driverName)) {
      throw new LensException("Driver Type and Name can not be null or empty");
    }
    fullyQualifiedName = new StringBuilder(driverType).append(SEPARATOR).append(driverName).toString();
  }

  /**
   * Gets the path (relative to lens server's conf location) for the driver resource in the system. This is a utility
   * method that can be used by extending driver implementations to build path for their resources.
   *
   * @param resourceName
   * @return
   */
  protected String getDriverResourcePath(String resourceName) {
    return new StringBuilder(LensConfConstants.DRIVERS_BASE_DIR).append(SEPARATOR).append(getFullyQualifiedName())
      .append(SEPARATOR).append(resourceName).toString();
  }

  @Override
  public Priority decidePriority(QueryContext queryContext) {
    // no-op by default
    return null;
  }

  @Override
  public String toString() {
    return getFullyQualifiedName();
  }
}
