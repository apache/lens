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
import org.apache.lens.api.metastore.XStorage;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import lombok.NonNull;

/**
 * The Class LensStorageCommands.
 */
@Component
@UserDocumentation(title = "Commands for Storage Management", description = "These commands provide CRUD for Storages")
public class LensStorageCommands extends LensCRUDCommand<XStorage> implements CommandMarker {

  @CliCommand(value = "show storages", help = "list all storages")
  public String getStorages() {
    return showAll();
  }

  /**
   * Creates the storage.
   *
   * @param path the storage spec path
   * @return the string
   */
  @CliCommand(value = "create storage", help = "Create a new Storage from file <path-to-storage-spec>")
  public String createStorage(
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-storage-spec>") @NonNull final File path) {
    return create(path, false);
  }

  /**
   * Drop storage.
   *
   * @param name the storage name
   * @return the string
   */
  @CliCommand(value = "drop storage", help = "drop storage <storage-name>")
  public String dropStorage(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<storage-name>") String name) {
    return drop(name, false);
  }

  /**
   * Update storage.
   *
   * @param name the storage name
   * @param path the new storage spec path
   * @return the string
   */
  @CliCommand(value = "update storage",
    help = "update storage <storage-name> with storage spec from <path-to-storage-spec>")
  public String updateStorage(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<storage-name>") String name,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-storage-spec>") @NonNull final File path) {
    return update(name, path);
  }

  /**
   * Describe storage.
   *
   * @param name the storage name
   * @return the string
   */
  @CliCommand(value = "describe storage", help = "describe storage <storage-name>")
  public String describeStorage(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<storage-name>") String name) {
    return describe(name);
  }

  @Override
  public List<String> getAll() {
    return getClient().getAllStorages();
  }

  @Override
  protected APIResult doCreate(String path, boolean ignoreIfExists) {
    return getClient().createStorage(path);
  }

  @Override
  protected XStorage doRead(String name) {
    return getClient().getStorage(name);
  }

  @Override
  public APIResult doUpdate(String name, String path) {
    return getClient().updateStorage(name, path);
  }

  @Override
  protected APIResult doDelete(String name, boolean cascade) {
    return getClient().dropStorage(name);
  }
}
