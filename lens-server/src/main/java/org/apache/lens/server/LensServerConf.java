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
package org.apache.lens.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * The Class LensServerConf.
 */
public final class LensServerConf {
  private LensServerConf() {

  }

  private static final class ConfHolder {
    public static final HiveConf HIVE_CONF = new HiveConf();
    // configuration object which does not load defaults and loads only lens*.xml files.
    public static final Configuration CONF = new Configuration(false);

    static {
      HIVE_CONF.addResource("lensserver-default.xml");
      HIVE_CONF.addResource("lens-site.xml");
      CONF.addResource("lensserver-default.xml");
      CONF.addResource("lens-site.xml");
    }
  }

  /**
   * The HiveConf object with lensserver-default.xml and lens-site.xml added.
   *
   * @return the hive conf
   */
  public static HiveConf getHiveConf() {
    return ConfHolder.HIVE_CONF;
  }

  /**
   * The configuration object which does not load any defaults and loads only lens*.xml files. This is passed to
   * all drivers in configure
   *
   * @return the conf
   */
  public static Configuration getConf() {
    return ConfHolder.CONF;
  }

  /**
   * Creates a new configuration object from Server HiveConf, Creation should would be called usually from tests
   * to modify some configurations.
   *
   * @return
   */
  public static HiveConf createHiveConf() {
    return new HiveConf(ConfHolder.HIVE_CONF);
  }
}
