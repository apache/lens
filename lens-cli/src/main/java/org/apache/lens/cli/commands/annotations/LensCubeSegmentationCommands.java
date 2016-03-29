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
package org.apache.lens.cli.commands.annotations;


import java.io.File;
import java.util.List;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.metastore.XCubeSegmentation;
import org.apache.lens.cli.commands.BaseTableCrudCommand;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import lombok.NonNull;

@Component
@UserDocumentation(title = "Commands for CubeSegmentation Management",
        description = "These command provide CRUD for CubeSegmentation")
public class LensCubeSegmentationCommands extends BaseTableCrudCommand<XCubeSegmentation> {

  /**
   * Show cube segmentation
   *
   * @return the string
   */
  @CliCommand(value = "show cubesegmentations",
      help = "display list of cubesegmentations in current database. "
          + "If optional <cube_name> is supplied, only cubesegmentations "
          + "belonging to cube <cube_name> will be displayed")
  public String showCubeSegmentations(
      @CliOption(key = {"", "cube_name"}, mandatory = false, help = "<cube_name>") String cubeName) {
    return showAll(cubeName);
  }
  /**
   * Creates the cubesegmentation
   *
   * @param path the cubesegmentation spec
   * @return the string
   */
  @CliCommand(value = "create cubesegmentation",
      help = "create a new cubesegmentation, taking spec from <path-to-cubesegmentation-spec file>")
  public String createCubeSegmentation(
      @CliOption(key = {"", "path"}, mandatory = true, help =
          "<path-to-cubesegmentation-spec file>") @NonNull final File path) {
    return create(path, false);
  }

  /**
   * Describe cubesegmentation.
   *
   * @param name the cubesegmentation name
   * @return the string
   */
  @CliCommand(value = "describe cubesegmentation", help = "describe cubesegmentation <cubesegmentation_name>")
  public String describeCubeSegmentation(
      @CliOption(key = {"", "name"}, mandatory = true, help = "<cubesegmentation_name>") String name) {
    return formatJson(getClient().getCubeSegmentation(name));
  }


  /**
   * Update cubesegmentation.
   *
   * @param name  the cubesegmentation to be updated
   * @param path  path to spec file
   * @return the string
   */
  @CliCommand(value = "update cubesegmentation",
      help = "update cubesegmentation <cubesegmentation_name>, taking spec from <path-to-cubesegmentation-spec file>")
  public String updateCubeSegmentation(
      @CliOption(key = {"", "name"}, mandatory = true, help = "<cubesegmentation_name>") String name,
      @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-cubesegmentation-spec-file>")
      @NonNull final File path) {
    return update(name, path);
  }

  /**
   * Drop cubesegmentation.
   *
   * @param name the cubesegmentation to be dropped
   * @return the string
   */
  @CliCommand(value = "drop cubesegmentation", help = "drop cubesegmentation <cubesegmentation_name>")
  public String dropCubeSegmentation(
      @CliOption(key = {"", "name"}, mandatory = true, help = "<cubesegmentation_name>") String name) {
    return drop(name, false);
  }

  @Override
  public List<String> getAll() {
    return getClient().getAllCubeSegmentations();
  }

  @Override
  protected APIResult doCreate(String path, boolean ignoreIfExists) {
    return getClient().createCubeSegmentation(path);
  }

  @Override
  protected XCubeSegmentation doRead(String name) {
    return getClient().getCubeSegmentation(name);
  }

  @Override
  public APIResult doUpdate(String name, String path) {
    return getClient().updateCubeSegmentation(name, path);
  }

  @Override
  protected APIResult doDelete(String name, boolean cascade) {
    return getClient().dropCubeSegmentation(name);
  }

  @Override
  protected List<String> getAll(String filter) {
    return getClient().getAllCubeSegmentations(filter);
  }
}
