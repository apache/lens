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

import java.util.List;

import javax.ws.rs.ProcessingException;

import org.apache.lens.api.APIResult;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.shell.core.ExitShellRequest;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;

/**
 * The Class LensConnectionCommands.
 */
@Component
@UserDocumentation(title = "Session management",
  description = "Opening the lens CLI shell is equivalent to open a session with lens server."
    + "This section provides all the commands available for in shell which are applicable for the full session.")
public class LensConnectionCommands extends BaseLensCommand {

  /**
   * Sets the param.
   *
   * @param keyval the keyval
   * @return the string
   */
  @CliCommand(value = "set", help = "Assign <value> to session parameter specified with <key> on lens server")
  public String setParam(@CliOption(key = {""}, mandatory = true, help = "<key>=<value>") String keyval) {
    String[] pair = keyval.split("=");
    if (pair.length != 2) {
      return "Error: Pass parameter as <key>=<value>";
    }
    APIResult result = getClient().setConnectionParam(pair[0], pair[1]);
    return result.getMessage();
  }

  /**
   * Gets the param.
   *
   * @param param the param
   * @return the param
   */
  @CliCommand(value = "get", help = "Fetches and prints session parameter specified with name <key> from lens server")
  public String getParam(@CliOption(key = {"", "key"}, mandatory = true, help = "<key>") String param) {
    return Joiner.on("\n").skipNulls().join(getClient().getConnectionParam(param));
  }

  /**
   * Show parameters.
   *
   * @return the string
   */
  @CliCommand(value = "show params", help = "Fetches and prints all session parameter from lens server")
  public String showParameters() {
    List<String> params = getClient().getConnectionParam();
    return Joiner.on("\n").skipNulls().join(params);
  }

  /**
   * Adds the jar.
   *
   * @param path the path
   * @return the string
   */
  @CliCommand(value = "add jar", help = "Adds jar resource to the session")
  public String addJar(
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-jar-on-server-side>") String path) {
    APIResult result = getClient().addJarResource(path);
    return result.getMessage();
  }

  /**
   * Removes the jar.
   *
   * @param path the path
   * @return the string
   */
  @CliCommand(value = "remove jar", help = "Removes a jar resource from session")
  public String removeJar(
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-jar-on-server-side>") String path) {
    APIResult result = getClient().removeJarResource(path);
    return result.getMessage();
  }

  /**
   * Adds the file.
   *
   * @param path the path
   * @return the string
   */
  @CliCommand(value = "add file", help = "Adds a file resource to session")
  public String addFile(
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-file-on-server-side>") String path) {
    APIResult result = getClient().addFileResource(path);
    return result.getMessage();
  }

  /**
   * Removes the file.
   *
   * @param path the path
   * @return the string
   */
  @CliCommand(value = "remove file", help = "removes a file resource from session")
  public String removeFile(
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-file-on-server-side>") String path) {
    APIResult result = getClient().removeFileResource(path);
    return result.getMessage();
  }

  /**
   * List resources.
   *
   * @return the string
   */
  @CliCommand(value = "list resources",
    help = "list all resources from session. If type is provided, "
      + " lists resources of type <resource-type>. Valid values for type are jar and file.")
  public String listResources(@CliOption(key = {"", "type"}, mandatory = false,
    help = "<resource-type>") String type) {
    List<String> resources = getClient().listResources(type);
    if (resources == null) {
      return "No resources found";
    }
    return Joiner.on("\n").skipNulls().join(resources);
  }

  /**
   * Quit shell.
   *
   * @return the exit shell request
   */
  @CliCommand(value = {"close", "bye"}, help = "Releases all resources of the server session and exits the shell")
  public ExitShellRequest quitShell() {
    try {
      closeClientConnection();
      return ExitShellRequest.NORMAL_EXIT;
    } catch (ProcessingException e) {
      System.out.println(e.getMessage());
      LOG.error(e);
      return ExitShellRequest.FATAL_EXIT;
    }
  }
}
