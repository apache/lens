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
package org.apache.lens.cli.commands;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import javax.ws.rs.core.Response;

import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * The Class LensLogResourceCommands.
 * SUSPEND CHECKSTYLE CHECK InnerAssignmentCheck
 */
@Component
@UserDocumentation(title = "Access to Log Resouces",
  description = "This section provides commands for fetching logs under LENS_LOG_DIR.")
public class LensLogResourceCommands extends BaseLensCommand {

  @CliCommand(value = "show logs", help = "show logs for a given query handle or log file")
  public String getLogs(
    @CliOption(key = {"", "log_handle"}, mandatory = true, help = "log handle can be be query_handle for queries")
    String logFile, @CliOption(key = {"save_location"}, mandatory = false, help = "<save_location>") String location) {
    try {
      Response response = getClient().getLogs(logFile);
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        if (StringUtils.isBlank(location)) {
          OutputStream outStream = new PrintStream(System.out, true, "UTF-8");
          try (InputStream stream = response.readEntity(InputStream.class);) {
            IOUtils.copy(stream, outStream);
            return "printed complete log content";
          }
        } else {
          location = getValidPath(new File(location), true, true);
          String fileName = logFile;
          location = getValidPath(new File(location + File.separator + fileName), false, false);
          try (InputStream stream = response.readEntity(InputStream.class);
              FileOutputStream outStream = new FileOutputStream(new File(location));) {
            IOUtils.copy(stream, outStream);
            return "Saved to " + location;
          }
        }
      } else {
        return response.toString();
      }
    } catch (Throwable t) {
      return t.getMessage();
    }
  }
}
