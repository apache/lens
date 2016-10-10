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
import org.apache.lens.api.metastore.XDimensionTable;
import org.apache.lens.api.metastore.XPartition;
import org.apache.lens.api.metastore.XStorageTableElement;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import lombok.NonNull;

/**
 * The Class LensDimensionTableCommands.
 */
@Component
@UserDocumentation(title = "Commands for Dimension Tables Management",
  description = "These commands provide CRUD for dimension tables, associated storages, and fact partitions")
public class LensDimensionTableCommands extends LogicalTableCrudCommand<XDimensionTable>
  implements CommandMarker {

  /**
   * Show dimension tables.
   *
   * @return the string
   */
  @CliCommand(value = "show dimtables",
    help = "display list of dimtables in current database. If optional <dimension_name> is supplied,"
      + " only facts belonging to dimension <dimension_name> will be displayed")
  public String showDimensionTables(
    @CliOption(key = {"", "dimension_name"}, mandatory = false, help = "<dimension_name>") String dimensionName) {
    return showAll(dimensionName);
  }

  /**
   * Creates the dimension table.
   *
   * @param path Path to dim spec
   * @return the string
   */
  @CliCommand(value = "create dimtable",
    help = "Create a new dimension table taking spec from <path-to-dimtable-spec-file>")
  public String createDimensionTable(
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-dimtable-spec-file>") @NonNull final File path) {
    return create(path, false);
  }

  /**
   * Describe dimension table.
   *
   * @param name the dim
   * @return the string
   */
  @CliCommand(value = "describe dimtable", help = "describe dimtable <dimtable_name>")
  public String describeDimensionTable(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String name) {
    return describe(name);
  }

  /**
   * Update dimension table.
   *
   * @param name the dimtable name
   * @param path the path to spec file
   * @return the string
   */
  @CliCommand(value = "update dimtable",
    help = "update dimtable <dimtable_name> taking spec from <path-to-dimtable-spec>")
  public String updateDimensionTable(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String name,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-dimtable-spec>") @NonNull final File path) {
    return update(name, path);
  }


  /**
   * Drop dimension table.
   *
   * @param name    the dim
   * @param cascade the cascade
   * @return the string
   */
  @CliCommand(value = "drop dimtable",
    help = "drop dimtable <dimtable_name>. "
      + " If <cascade> is true, all the storage tables associated with the dimtable <dimtable_name> are also dropped."
      + " By default <cascade> is false")
  public String dropDimensionTable(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String name,
    @CliOption(key = {"cascade"}, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
      help = "<cascade>")
    boolean cascade) {
    return drop(name, cascade);
  }

  /**
   * Gets the dim storages.
   *
   * @param table the dim
   * @return the dim storages
   */
  @CliCommand(value = "dimtable list storages", help = "display list of storage associated to dimtable <dimtable_name>")
  public String getDimStorages(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String table) {
    return showAllStorages(table);
  }

  /**
   * Adds the new dim storage.
   *
   * @param tableName dimtable name
   * @param path      path to storage spec
   * @return the string
   */
  @CliCommand(value = "dimtable add storage",
    help = "adds a new storage to dimtable <dimtable_name>, taking storage spec from <path-to-storage-spec>")
  public String addNewDimStorage(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-storage-spec>") @NonNull final File path) {
    return addStorage(tableName, path);
  }

  /**
   * Gets the storage from dim.
   *
   * @param tableName dimtable name
   * @return path storage spec path
   */
  @CliCommand(value = "dimtable get storage", help = "describe storage <storage_name> of dimtable <dimtable_name>")
  public String getStorageFromDim(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storage) {
    return getStorage(tableName, storage);
  }

  /**
   * Drop storage from dim.
   *
   * @param tableName   dimtable name
   * @param storageName storage name
   * @return the string
   */
  @CliCommand(value = "dimtable drop storage", help = "drop storage <storage_name> from dimtable <dimtable_name>")
  public String dropStorageFromDim(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName) {
    return dropStorage(tableName, storageName);
  }

  /**
   * Drop all dim storages.
   *
   * @param tableName the table
   * @return the string
   */
  @CliCommand(value = "dimtable drop all storages", help = "drop all storages associated to dimension table")
  public String dropAllDimStorages(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName) {
    return dropAllStorages(tableName);
  }

  /**
   * Gets the all partitions of dim.
   *
   * @param tableName   dimtable name
   * @param storageName storage name
   * @param filter      partition filter
   * @return the all partitions of dim
   */
  @CliCommand(value = "dimtable list partitions",
    help = "get all partitions associated with dimtable <dimtable_name>, "
      + "storage <storage_name> filtered by <partition-filter>")
  public String getAllPartitionsOfDimtable(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "filter"}, mandatory = false, help = "<partition-filter>") String filter) {
    return getAllPartitions(tableName, storageName, filter);
  }

  /**
   * Drop all partitions of dim.
   *
   * @param tableName   dimtable name
   * @param storageName storage name
   * @param filter      partition query filter
   * @return the string
   */
  @CliCommand(value = "dimtable drop partitions",
    help = "drop all partitions associated with dimtable "
      + "<dimtable_name>, storage <storage_name> filtered by <partition-filter>")
  public String dropAllPartitionsOfDim(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "filter"}, mandatory = false, help = "<partition-filter>") String filter) {
    return dropPartitions(tableName, storageName, filter);
  }

  /**
   * Adds the partition to dim table.
   *
   * @param tableName   dimtable name
   * @param storageName storage name
   * @param path        partition spec path
   * @return the string
   */
  @CliCommand(value = "dimtable add single-partition",
    help = "add single partition to dimtable <dimtable_name>'s"
      + " storage <storage_name>, reading spec from <partition-spec-path>")
  public String addPartitionToDimtable(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<partition-spec-path>") @NonNull final File path) {
    return addPartition(tableName, storageName, path);
  }
  @CliCommand(value = "dimtable update single-partition",
    help = "update single partition to dimtable <dimtable_name>'s"
      + " storage <storage_name>, reading spec from <partition-spec-path>"
      + " The partition has to exist to be eligible for updation.")
  public String updatePartitionOfDimtable(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<partition-spec-path>") @NonNull final File path) {
    return updatePartition(tableName, storageName, path);
  }

  /**
   * Adds the partitions to dim table.
   *
   * @param tableName   dimtable name
   * @param storageName storage name
   * @param path        partition spec path
   * @return the string
   */

  @CliCommand(value = "dimtable add partitions",
    help = "add multiple partition to dimtable <dimtable_name>'s"
      + " storage <storage_name>, reading partition list spec from <partition-list-spec-path>")
  public String addPartitionsToDimtable(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<partition-list-spec-path>") @NonNull final File path) {
    return addPartitions(tableName, storageName, path.getPath());
  }
  @CliCommand(value = "dimtable update partitions",
    help = "update multiple partition to dimtable <dimtable_name>'s"
      + " storage <storage_name>, reading partition list spec from <partition-list-spec-path>"
      +" The partitions have to exist to be eligible for updation.")
  public String updatePartitionsOfDimtable(
    @CliOption(key = {"", "dimtable_name"}, mandatory = true, help = "<dimtable_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<partition-list-spec-path>") String path) {
    return updatePartitions(tableName, storageName, path);
  }

  @Override
  protected XStorageTableElement readStorage(String tableName, String storage) {
    return getClient().getStorageFromDim(tableName, storage);
  }

  @Override
  public APIResult doDropStorage(String tableName, String storageName) {
    return getClient().dropStorageFromDim(tableName, storageName);
  }


  @Override
  public List<String> getAllStorages(String name) {
    return getClient().getDimStorages(name);
  }

  @Override
  public APIResult doAddStorage(String name, String path) {
    return getClient().addStorageToDim(name, path);
  }

  @Override
  public APIResult doDropAllStorages(String name) {
    return getClient().dropAllStoragesOfDim(name);
  }

  @Override
  protected List<XPartition> readAllPartitions(String tableName, String storageName, String filter) {
    return getClient().getAllPartitionsOfDim(tableName, storageName, filter);
  }

  @Override
  protected APIResult doAddPartition(String tableName, String storageName, String path) {
    return getClient().addPartitionToDim(tableName, storageName, path);
  }

  @Override
  protected APIResult doAddPartitions(String tableName, String storageName, String path) {
    return getClient().addPartitionsToDim(tableName, storageName, path);
  }

  @Override
  protected APIResult doDropPartitions(String tableName, String storageName, String filter) {
    return getClient().dropAllPartitionsOfDim(tableName, storageName, filter);
  }

  @Override
  protected APIResult doUpdatePartition(String tableName, String storageName, String validPath) {
    return getClient().updatePartitionOfDim(tableName, storageName, validPath);
  }

  @Override
  protected APIResult doUpdatePartitions(String tableName, String storageName, String validPath) {
    return getClient().updatePartitionsOfDim(tableName, storageName, validPath);
  }

  @Override
  public List<String> getAll() {
    return getClient().getAllDimensionTables();
  }

  @Override
  public List<String> getAll(String dimesionName) {
    return getClient().getAllDimensionTables(dimesionName);
  }

  @Override
  protected APIResult doCreate(String path, boolean ignoreIfExists) {
    return getClient().createDimensionTable(path);
  }

  @Override
  protected XDimensionTable doRead(String name) {
    return getClient().getDimensionTable(name);
  }

  @Override
  public APIResult doUpdate(String name, String path) {
    return getClient().updateDimensionTable(name, path);
  }

  @Override
  protected APIResult doDelete(String name, boolean cascade) {
    return getClient().dropDimensionTable(name, cascade);
  }
}
