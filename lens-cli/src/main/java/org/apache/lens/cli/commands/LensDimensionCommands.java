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
import java.util.List;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.metastore.XDimension;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import lombok.NonNull;

/**
 * The Class LensDimensionCommands.
 */
@Component
@UserDocumentation(title = "Commands for Dimension Management",
  description = "These commands provide CRUD for Dimensions")
public class LensDimensionCommands extends ConceptualTableCrudCommand<XDimension> {

  /**
   * Show dimensions.
   *
   * @return the string
   */
  @CliCommand(value = "show dimensions", help = "show list of all dimensions in current database")
  public String showDimensions() {
    return showAll();
  }

  /**
   * Creates the dimension.
   *
   * @param path the dimension spec
   * @return the string
   */
  @CliCommand(value = "create dimension",
    help = "Create a new Dimension, taking spec from <path-to-dimension-spec file>")
  public String createDimension(
    @CliOption(key = {"", "path"}, mandatory = true, help =
      "<path-to-dimension-spec file>") @NonNull final File path) {
    return create(path, false);
  }

  /**
   * Describe dimension.
   *
   * @param name the dimension name
   * @return the string
   */
  @CliCommand(value = "describe dimension", help = "describe dimension <dimension_name>")
  public String describeDimension(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<dimension_name>") String name) {
    return formatJson(getClient().getDimension(name));
  }

  /**
   * Update dimension.
   *
   * @param name the dimension to be updated
   * @param path  path to spec fild
   * @return the string
   */
  @CliCommand(value = "update dimension",
    help = "update dimension <dimension_name>, taking spec from <path-to-dimension-spec file>")
  public String updateDimension(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<dimension_name>") String name,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-dimension-spec-file>") @NonNull final File path) {
    return update(name, path);
  }

  /**
   * Drop dimension.
   *
   * @param name the dimension
   * @return the string
   */
  @CliCommand(value = "drop dimension", help = "drop dimension <dimension_name>")
  public String dropDimension(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<dimension_name>") String name) {
    return drop(name, false);
  }

  @CliCommand(value = "dimension show fields",
    help = "Show queryable fields of the given dimension <dimension_name>. "
      + "Optionally specify <flattened> to include chained fields")
  public String showQueryableFields(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<dimension_name>") String table,
    @CliOption(key = {"flattened"}, mandatory = false, unspecifiedDefaultValue = "false",
      specifiedDefaultValue = "true", help = "<flattened>") boolean flattened) {
    return getAllFields(table, flattened);
  }

  @CliCommand(value = "dimension show joinchains",
    help = "Show joinchains of the given dimension <dimension_name>. ")
  public String showJoinChains(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<dimension_name>") String table) {
    return getAllJoinChains(table);
  }

  @Override
  public List<String> getAll() {
    return getClient().getAllDimensions();
  }

  @Override
  protected APIResult doCreate(String path, boolean ignoreIfExists) {
    return getClient().createDimension(path);
  }

  @Override
  protected XDimension doRead(String name) {
    return getClient().getDimension(name);
  }

  @Override
  public APIResult doUpdate(String name, String path) {
    return getClient().updateDimension(name, path);
  }

  @Override
  protected APIResult doDelete(String name, boolean cascade) {
    return getClient().dropDimension(name);
  }
}
