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

package org.apache.lens.cli.config;

public class LensCliConfigConstants {

  private LensCliConfigConstants(){
  }

  public static final String LENS_CLI_PREFIX = "lens.cli.";

  public static final String PRINT_PRETTY_JSON = LENS_CLI_PREFIX + "json.pretty";

  public static final boolean DEFAULT_PRINT_PRETTY_JSON = false;

  public static final String QUERY_EXECUTE_TIMEOUT_MILLIS = LENS_CLI_PREFIX  + "query.execute.timeout.millis";

  public static final long DEFAULT_QUERY_EXECUTE_TIMEOUT_MILLIS = 10000; //10 secs
}
