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


import static org.apache.lens.server.api.LensConfConstants.DRIVER_PFX;

import org.apache.hadoop.conf.Configuration;

public class DriverConfiguration extends Configuration {
  private final String driverClassType;
  private String driverType;
  private final Class<? extends AbstractLensDriver> driverClass;

  public DriverConfiguration(Configuration conf, String driverType, Class<? extends AbstractLensDriver> driverClass) {
    super(conf);
    this.driverType = driverType;
    this.driverClass = driverClass;
    this.driverClassType = driverClass.getSimpleName().toLowerCase().replaceAll("driver$", "");
  }
  public DriverConfiguration(String driverType, Class<? extends AbstractLensDriver> driverClass) {
    this.driverType = driverType;
    this.driverClass = driverClass;
    this.driverClassType = driverClass.getSimpleName().toLowerCase().replaceAll("driver$", "");
  }

  @Override
  public String get(String name) {
    String[] prefixes = new String[]{DRIVER_PFX + driverType + ".", DRIVER_PFX + driverClassType + ".", DRIVER_PFX, };
    for (String prefix : prefixes) {
      if (name.startsWith(prefix)) {
        return getInternal(name.substring(prefix.length()));
      }
    }
    return getInternal(name);
  }

  public String getInternal(String name) {
    for (String key : new String[]{DRIVER_PFX + driverType + "." + name, DRIVER_PFX + driverClassType + "." + name,
      DRIVER_PFX + name, name, }) {
      String val = super.get(key);
      if (val != null) {
        return val;
      }
    }
    return null;
  }
}
