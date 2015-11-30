/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es;

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.BeforeTest;

public abstract class ESDriverTest {

  protected Configuration config = new Configuration();
  protected ESDriverConfig esDriverConfig;
  protected ESDriver driver = new ESDriver();
  protected MockClientES mockClientES;

  @BeforeTest
  public void beforeTest() throws LensException {
    initializeConfig(config);
    esDriverConfig = new ESDriverConfig(config);
    driver.configure(config, "es", "es1");
    mockClientES = (MockClientES) driver.getESClient();
  }

  protected abstract void initializeConfig(Configuration config);
}
