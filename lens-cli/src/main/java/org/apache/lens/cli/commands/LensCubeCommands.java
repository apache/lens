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

import java.util.Date;
import java.util.List;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.metastore.XCube;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * The Class LensCubeCommands.
 */
@Component
@UserDocumentation(title = "OLAP Data cube metadata management",
  description = "These commands provide CRUD for cubes")
public class LensCubeCommands extends LensCRUDCommand<XCube> {

  /**
   * Show cubes.
   *
   * @return the string
   */
  @CliCommand(value = "show cubes", help = "show list of cubes in current database")
  public String showCubes() {
    return showAll();
  }

  /**
   * Creates the cube.
   *
   * @param path the cube spec
   * @return the string
   */
  @CliCommand(value = "create cube", help = "Create a new Cube, taking spec from <path-to-cube-spec-file>")
  public String createCube(
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-cube-spec-file>") String path) {
    return create(path, false);
  }

  /**
   * Describe cube.
   *
   * @param name the cube name
   * @return the string
   */
  @CliCommand(value = "describe cube", help = "describe cube with name <cube-name>")
  public String describeCube(@CliOption(key = {"", "name"}, mandatory = true, help = "<cube-name>") String name) {
    return describe(name);
  }

  /**
   * Update cube.
   *
   * @param name     cube name
   * @param path path to new spec file
   * @return the string
   */
  @CliCommand(value = "update cube", help = "update cube <cube-name> with spec from <path-to-cube-spec-file>")
  public String updateCube(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<cube-name>") String name,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-cube-spec-file>") String path) {
    return update(name, path);
  }

  /**
   * Drop cube.
   *
   * @param name the cube
   * @return the string
   */
  @CliCommand(value = "drop cube", help = "drop cube <cube-name>")
  public String dropCube(@CliOption(key = {"", "name"}, mandatory = true, help = "<cube-name>") String name) {
    return drop(name, false);
  }

  /**
   * Cube latest date
   *
   * @param cube cube name
   * @param timeDim time dimension name
   * @return the string
   */
  @CliCommand(value = "cube latestdate",
    help = "get latest date of data available in cube <cube-name> for time dimension <time-dimension-name>")
  public String getLatest(
    @CliOption(key = {"", "cube"}, mandatory = true, help = "<cube-name>") String cube,
    @CliOption(key = {"", "timeDimension"}, mandatory = true, help = "<time-dimension-name>") String timeDim) {
    Date dt = getClient().getLatestDateOfCube(cube, timeDim);
    return dt == null ? "No Data Available" : formatDate(dt);
  }

  @Override
  public List<String> getAll() {
    return getClient().getAllCubes();
  }

  @Override
  protected APIResult doCreate(String path, boolean ignoreIfExists) {
    return getClient().createCube(path);
  }

  @Override
  protected APIResult doDelete(String name, boolean cascade) {
    return getClient().dropCube(name);
  }

  @Override
  public APIResult doUpdate(String name, String path) {
    return getClient().updateCube(name, path);
  }

  @Override
  protected XCube doRead(String name) {
    return getClient().getCube(name);
  }

}
