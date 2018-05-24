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

package org.apache.lens.client;

import static org.apache.lens.api.auth.AuthHeader.HDFS_DELEGATION_TKN_HEADER;

import java.io.IOException;
import java.util.Optional;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A client filter for Jersey client which forewards delegation token in {@code X-Hadoop-Delegation-Token} header.
 *
 * Currently the HDFS_DELEGATION_TOKEN is sent.
 */
public class DelegationTokenClientFilter implements ClientRequestFilter {
  private static final String DELEGATION_TKN_KIND = "HDFS_DELEGATION_TOKEN";

  @Override
  public void filter(ClientRequestContext requestContext) throws IOException {
    Optional<Token<? extends TokenIdentifier>> hdfsDelegationToken = UserGroupInformation
      .getLoginUser().getTokens().stream()
      .filter(tkn -> tkn.getKind().toString().equals(DELEGATION_TKN_KIND))
      .findFirst();

    if (hdfsDelegationToken.isPresent()) {
      requestContext.getHeaders().add(HDFS_DELEGATION_TKN_HEADER, hdfsDelegationToken.get().encodeToUrlString());
    }
  }
}
