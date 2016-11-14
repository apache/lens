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
/*
 *
 */
package org.apache.lens.server.api.driver.hooks;

import java.util.Set;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;

import com.google.common.collect.Sets;

/**
 * This hook allows for a driver to discard queries based on the submitter of the query. This works on two
 * configurations values, among which either or none can be set. The configuration values are picked from
 * driver configuration. The driver config can set `allowed.users` as a comma separated list of users that
 * are allowed to submit queries on this driver. Queries from only those users will be allowed
 * and the rest will be discarded. On the other hand, the driver can set `disallowed.users` as a
 * comma separated list of users that are disallowed to submit queries on this driver. Queries from these users
 * will be rejected, while queries from any other user will be accepted.
 */
public class UserBasedQueryHook extends NoOpDriverQueryHook {
  public static final String ALLOWED_USERS = "allowed.users";
  public static final String DISALLOWED_USERS = "disallowed.users";
  private Set<String> allowedUsers;
  private Set<String> disallowedUsers;

  @Override
  public void setDriver(LensDriver driver) {
    super.setDriver(driver);
    String[] allowedUsers = driver.getConf().getStrings(ALLOWED_USERS);
    String[] disallowedUsers = driver.getConf().getStrings(DISALLOWED_USERS);
    if (allowedUsers != null && disallowedUsers != null) {
      throw new IllegalStateException("You can't specify both allowed users and disallowed users");
    }
    this.allowedUsers = allowedUsers == null ? null : Sets.newHashSet(allowedUsers);
    this.disallowedUsers = disallowedUsers == null ? null : Sets.newHashSet(disallowedUsers);
  }

  private String getErrorMessage(AbstractQueryContext ctx) {
    return "User " + ctx.getSubmittedUser() + " not allowed to submit query on driver "
      + getDriver().getFullyQualifiedName();
  }

  @Override
  public void preRewrite(AbstractQueryContext ctx) throws LensException {
    if ((allowedUsers != null && !allowedUsers.contains(ctx.getSubmittedUser()))
      || (disallowedUsers != null && disallowedUsers.contains(ctx.getSubmittedUser()))) {
      throw new LensException(getErrorMessage(ctx));
    }
  }
}
