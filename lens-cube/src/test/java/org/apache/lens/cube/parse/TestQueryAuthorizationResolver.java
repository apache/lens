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
package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.lens.cube.metadata.MetastoreConstants;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.save.exception.PrivilegeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestQueryAuthorizationResolver extends TestQueryRewrite {
  private Configuration conf = new Configuration();

  @BeforeClass
  public void beforeClassTestQueryAuthorizationResolver() {
    conf.setBoolean(LensConfConstants.ENABLE_QUERY_AUTHORIZATION_CHECK, true);
    conf.setBoolean(LensConfConstants.USER_GROUPS_BASED_AUTHORIZATION, true);
    conf.set(MetastoreConstants.AUTHORIZER_CLASS, "org.apache.lens.cube.parse.MockAuthorizer");
  }

  @Test
  public void testRestrictedColumnsFromQuery() throws LensException {

    SessionState.getSessionConf().set(LensConfConstants.SESSION_USER_GROUPS, "lens-auth-test2");
    String testQuery = "select dim11 from basecube where " + TWO_DAYS_RANGE;

    try {
      rewrite(testQuery, conf);
      fail("Privilege exception supposed to be thrown for selecting restricted columns in basecube, "
         + "however not seeing expected behaviour");
    } catch (PrivilegeException actualException) {
      PrivilegeException expectedException =
        new PrivilegeException("COLUMN", "basecube", "SELECT");
      assertEquals(expectedException, actualException);
    }
    SessionState.getSessionConf().set(LensConfConstants.SESSION_USER_GROUPS, "lens-auth-test1");
    rewrite(testQuery, conf);
  }

}
