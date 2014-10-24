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

import com.google.common.base.Joiner;

import org.apache.lens.api.APIResult;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.ExitShellRequest;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The Class LensConnectionCommands.
 */
@Component
public class LensConnectionCommands extends BaseLensCommand implements CommandMarker {

  /**
   * Sets the param.
   *
   * @param keyval
   *          the keyval
   * @return the string
   */
  @CliCommand(value = "set", help = "sets a session parameter.")
  public String setParam(@CliOption(key = { "", "param" }, mandatory = true, help = "key=val") String keyval) {
    String[] pair = keyval.split("=");
    if (pair.length != 2) {
      return "Error: Pass parameter as <key>=<value>";
    }
    APIResult result = getClient().setConnectionParam(pair[0], pair[1]);
    return result.getMessage();
  }

  /**
   * Show parameters.
   *
   * @return the string
   */
  @CliCommand(value = "show params", help = "list of all session parameter")
  public String showParameters() {
    List<String> params = getClient().getConnectionParam();
    return Joiner.on("\n").skipNulls().join(params);
  }

  /**
   * Gets the param.
   *
   * @param param
   *          the param
   * @return the param
   */
  @CliCommand(value = "get", help = "gets value of session parameter")
  public String getParam(@CliOption(key = { "", "param" }, mandatory = true, help = "param name") String param) {
    return Joiner.on("\n").skipNulls().join(getClient().getConnectionParam(param));
  }

  /**
   * Adds the jar.
   *
   * @param path
   *          the path
   * @return the string
   */
  @CliCommand(value = "add jar", help = "adds a jar resource to session")
  public String addJar(
      @CliOption(key = { "", "param" }, mandatory = true, help = "path to jar on serverside") String path) {
    APIResult result = getClient().addJarResource(path);
    return result.getMessage();
  }

  /**
   * Removes the jar.
   *
   * @param path
   *          the path
   * @return the string
   */
  @CliCommand(value = "remove jar", help = "removes a jar resource from session")
  public String removeJar(
      @CliOption(key = { "", "param" }, mandatory = true, help = "path to jar on serverside") String path) {
    APIResult result = getClient().removeJarResource(path);
    return result.getMessage();
  }

  /**
   * Adds the file.
   *
   * @param path
   *          the path
   * @return the string
   */
  @CliCommand(value = "add file", help = "adds a file resource to session")
  public String addFile(
      @CliOption(key = { "", "param" }, mandatory = true, help = "path to file on serverside") String path) {
    APIResult result = getClient().addFileResource(path);
    return result.getMessage();
  }

  /**
   * Removes the file.
   *
   * @param path
   *          the path
   * @return the string
   */
  @CliCommand(value = "remove file", help = "removes a file resource from session")
  public String removeFile(
      @CliOption(key = { "", "param" }, mandatory = true, help = "path to file on serverside") String path) {
    APIResult result = getClient().removeFileResource(path);
    return result.getMessage();
  }

  /**
   * Quit shell.
   *
   * @return the exit shell request
   */
  @CliCommand(value = { "close" }, help = "Exits the shell")
  public ExitShellRequest quitShell() {
    closeClientConnection();
    return ExitShellRequest.NORMAL_EXIT;
  }
}
