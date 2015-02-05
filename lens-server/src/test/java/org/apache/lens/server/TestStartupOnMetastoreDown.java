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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;

import org.testng.Assert;

public class TestStartupOnMetastoreDown {
  private static final Log LOG = LogFactory.getLog(TestStartupOnMetastoreDown.class);

  // @Test
  public void testServicesStartOnMetastoreDown() throws Exception {
    LensServices services = new LensServices(LensServices.LENS_SERVICES_NAME);
    HiveConf hiveConf = new HiveConf();

    // Set metastore uri to an invalid location
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:49153");

    try {
      services.init(hiveConf);
      Assert.fail("Expected init to fail because of invalid metastore config");
    } catch (Throwable th) {
      Assert.assertTrue(th.getMessage().contains(
        "Unable to instantiate org.apache.hadoop.hive.metastore.HiveMetaStoreClient"));
    } finally {
      try {
        services.stop();
      } catch (Exception exc) {
        LOG.error("Error stopping services", exc);
        Assert.fail("services.stop() got unexpected exception " + exc);
      }
      Hive.closeCurrent();
    }
  }

}
