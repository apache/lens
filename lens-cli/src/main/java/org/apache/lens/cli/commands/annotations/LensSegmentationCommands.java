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
import org.apache.lens.api.metastore.XSegmentation;
import org.apache.lens.cli.commands.BaseTableCrudCommand;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import lombok.NonNull;

@Component
@UserDocumentation(title = "Commands for Segmentation Management",
        description = "These command provide CRUD for Segmentation")
public class LensSegmentationCommands extends BaseTableCrudCommand<XSegmentation> {

  /**
   * Show segmentation
   *
   * @return the string
   */
  @CliCommand(value = "show segmentations",
      help = "display list of segmentations in current database. "
          + "If optional <cube_name> is supplied, only segmentations "
          + "belonging to cube <cube_name> will be displayed")
  public String showSegmentations(
      @CliOption(key = {"", "cube_name"}, mandatory = false, help = "<cube_name>") String cubeName) {
    return showAll(cubeName);
  }
  /**
   * Creates the segmentation
   *
   * @param path the segmentation spec
   * @return the string
   */
  @CliCommand(value = "create segmentation",
      help = "create a new segmentation, taking spec from <path-to-segmentation-spec file>")
  public String createSegmentation(
      @CliOption(key = {"", "path"}, mandatory = true, help =
          "<path-to-segmentation-spec file>") @NonNull final File path) {
    return create(path, false);
  }

  /**
   * Describe segmentation.
   *
   * @param name the segmentation name
   * @return the string
   */
  @CliCommand(value = "describe segmentation", help = "describe segmentation <segmentation_name>")
  public String describeSegmentation(
      @CliOption(key = {"", "name"}, mandatory = true, help = "<segmentation_name>") String name) {
    return formatJson(getClient().getSegmentation(name));
  }


  /**
   * Update segmentation.
   *
   * @param name  the segmentation to be updated
   * @param path  path to spec file
   * @return the string
   */
  @CliCommand(value = "update segmentation",
      help = "update segmentation <segmentation_name>, taking spec from <path-to-segmentation-spec file>")
  public String updateSegmentation(
      @CliOption(key = {"", "name"}, mandatory = true, help = "<segmentation_name>") String name,
      @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-segmentation-spec-file>")
      @NonNull final File path) {
    return update(name, path);
  }

  /**
   * Drop segmentation.
   *
   * @param name the segmentation to be dropped
   * @return the string
   */
  @CliCommand(value = "drop segmentation", help = "drop segmentation <segmentation_name>")
  public String dropSegmentation(
      @CliOption(key = {"", "name"}, mandatory = true, help = "<segmentation_name>") String name) {
    return drop(name, false);
  }

  @Override
  public List<String> getAll() {
    return getClient().getAllSegmentations();
  }

  @Override
  protected APIResult doCreate(String path, boolean ignoreIfExists) {
    return getClient().createSegmentation(path);
  }

  @Override
  protected XSegmentation doRead(String name) {
    return getClient().getSegmentation(name);
  }

  @Override
  public APIResult doUpdate(String name, String path) {
    return getClient().updateSegmentation(name, path);
  }

  @Override
  protected APIResult doDelete(String name, boolean cascade) {
    return getClient().dropSegmentation(name);
  }

  @Override
  protected List<String> getAll(String filter) {
    return getClient().getAllSegmentations(filter);
  }
}
