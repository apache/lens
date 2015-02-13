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
package org.apache.lens.client;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Console;

import lombok.Data;

/**
 * The Class Credentials.
 * It's not a utility class. checkstyle thinks it is.
 * SUSPEND CHECKSTYLE CHECK HideUtilityClassConstructorCheck
 */
@Data
public class Credentials {

  /** The username. */
  private final String username;

  /** The password. */
  private final String password;

  /**
   * Prompt.
   *
   * @return the credentials
   */
  public static Credentials prompt() {
    Console console = System.console();
    if (console == null) {
      System.err.println("Couldn't get Console instance");
      System.exit(-1);
    }
    console.printf("username:");
    String username = console.readLine().trim();
    char[] passwordArray = console.readPassword("password for %s:", username);
    String password = new String(passwordArray);
    return new Credentials(username, password);
  }
}
// RESUME CHECKSTYLE CHECK HideUtilityClassConstructorCheck
