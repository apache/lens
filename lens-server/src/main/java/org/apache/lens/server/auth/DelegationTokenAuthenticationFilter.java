/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.auth;

import static org.apache.lens.api.auth.AuthHeader.HDFS_DELEGATION_TKN_HEADER;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;

import javax.annotation.Priority;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.api.LensConfConstants;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

import lombok.extern.slf4j.Slf4j;

/**
 * A JAX-RS filter for delegation token authentication.
 *
 * <p>Currently the delegation token of HDFS is received and is passed to HDFS using a FS operation to authenticate.
 * If FS operation returns normally, we assume the user is authenticated.
 * </p>
 *
 * <p>This filter can be enabled by adding an entry in {@code lens.server.ws.filternames} property and providing
 * the impl class.</p>
 *
 */
@Priority(10)
@Slf4j
public class DelegationTokenAuthenticationFilter implements ContainerRequestFilter {
  private static final String AUTH_SCHEME = "Digest-MD5";
  private static final org.apache.hadoop.conf.Configuration CONF = LensServerConf.getHiveConf();
  private static final Path PATH_TO_CHECK = new Path(
          CONF.get(LensConfConstants.DELEGATION_TOKEN_AUTH_HDFS_PATH_TO_CHECK));

  private ResourceInfo resourceInfo;;

  @Context
  public void setResourceInfo(ResourceInfo resourceInfo) {
    this.resourceInfo = resourceInfo;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    Principal userPrincipal = requestContext.getSecurityContext().getUserPrincipal();
    if (userPrincipal != null) {
      log.info("Authentication already done for principal {}, skipping this filter...", userPrincipal.getName());
      return;
    }
    // only authenticate when @Authenticate is present on resource
    if (resourceInfo.getResourceClass() == null || resourceInfo.getResourceMethod() == null) {
      return;
    }
    if (!(resourceInfo.getResourceClass().isAnnotationPresent(Authenticate.class)
      || resourceInfo.getResourceMethod().isAnnotationPresent(Authenticate.class))) {
      return;
    }

    String delegationToken = requestContext.getHeaderString(HDFS_DELEGATION_TKN_HEADER);
    if (StringUtils.isBlank(delegationToken)) {
      return;
    }

    Token<AbstractDelegationTokenIdentifier> dt = new Token();
    dt.decodeFromUrlString(delegationToken);
    UserGroupInformation user = dt.decodeIdentifier().getUser();
    user.addToken(dt);

    log.info("Received delegation token for user: {}", user.getUserName());

    try {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          try (FileSystem fs = FileSystem.get(new Configuration())) {
            fs.exists(PATH_TO_CHECK); // dummy hdfs call
            requestContext.setSecurityContext(createSecurityContext(user.getUserName(), AUTH_SCHEME));
            return null;
          }
        }
      });
    } catch (InterruptedException | IOException e) {
      log.error("Error while doing HDFS op: ", e);
      throw new NotAuthorizedException(Response.status(401).entity("Invalid HDFS delegation token").build());
    }
  }

  private SecurityContext createSecurityContext(String simpleUserName, String authScheme) {
    return new LensSecurityContext(simpleUserName, authScheme);
  }
}
