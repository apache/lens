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
package org.apache.lens.server.query;

import org.apache.lens.api.query.SubmitOp;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryAcceptor;

import org.apache.hadoop.conf.Configuration;

public class BlahQueryAcceptor implements QueryAcceptor {
  public static final String MSG = "Query can't start with blah";

  @Override
  public String accept(String query, Configuration conf, SubmitOp submitOp) throws LensException {
    if (query.toLowerCase().startsWith("blah")) {
      return MSG;
    }
    return null;
  }
}
