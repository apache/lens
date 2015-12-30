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
package org.apache.lens.server.api.query;

import java.util.Collection;
import java.util.List;

import org.apache.lens.api.LensConf;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.MockDriver;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.internal.Lists;

public class MockQueryContext extends QueryContext {

  private static final long serialVersionUID = 1L;

  public MockQueryContext(final String query, final LensConf qconf,
    final Configuration conf, final Collection<LensDriver> drivers) {
    super(query, "testuser", qconf, conf, drivers, drivers.iterator().next(), false);
  }

  public MockQueryContext() throws LensException {
    this(new Configuration());
  }

  public MockQueryContext(final Collection<LensDriver> drivers) throws LensException {
    this("mock query", new LensConf(), new Configuration(), drivers);
  }

  public MockQueryContext(Configuration conf) throws LensException {
    this("mock query", new LensConf(), conf, getDrivers(conf));
  }

  public static List<LensDriver> getDrivers(Configuration conf) throws LensException {
    List<LensDriver> drivers = Lists.newArrayList();
    MockDriver d = new MockDriver();
    d.configure(conf, null, null);
    drivers.add(d);
    return drivers;
  }

  @Override
  public String getLogHandle() {
    return super.getUserQuery();
  }
}
