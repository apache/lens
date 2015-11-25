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
package org.apache.lens.server.api;

import org.apache.lens.api.LensConf;

import org.apache.hadoop.conf.Configuration;

public class LensServerAPITestUtil {
  private LensServerAPITestUtil() {

  }

  public static Configuration getConfiguration(Object... args) {
    Configuration conf = new Configuration(false);
    assert (args.length % 2 == 0);
    for (int i = 0; i < args.length; i += 2) {
      conf.set(args[i].toString(), args[i + 1].toString());
    }
    return conf;
  }

  public static LensConf getLensConf(Object... args) {
    assert (args.length % 2 == 0);
    LensConf conf = new LensConf();
    for (int i = 0; i < args.length; i += 2) {
      conf.addProperty(args[i], args[i + 1]);
    }
    return conf;
  }
}
