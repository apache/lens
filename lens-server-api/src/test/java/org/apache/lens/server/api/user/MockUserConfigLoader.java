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
package org.apache.lens.server.api.user;

import java.util.Map;

import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.hive.conf.HiveConf;

import com.beust.jcommander.internal.Maps;

public class MockUserConfigLoader extends UserConfigLoader {
  public static final String KEY = "TEST_KEY";
  public static final String VALUE = "TEST_VALUE";

  public MockUserConfigLoader(HiveConf conf) {
    super(conf);
  }

  @Override
  public Map<String, String> getUserConfig(String loggedInUser) throws UserConfigLoaderException {
    return Maps.newHashMap();
  }

  @Override
  public void preSubmit(QueryContext ctx) throws LensException {
    ctx.getSelectedDriverConf().set(KEY, VALUE);
  }
}
