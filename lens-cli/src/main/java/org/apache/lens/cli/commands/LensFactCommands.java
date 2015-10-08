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
import org.apache.lens.api.metastore.XFactTable;
import org.apache.lens.api.metastore.XPartition;
import org.apache.lens.api.metastore.XStorageTableElement;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import lombok.NonNull;

/**
 * The Class LensFactCommands.
 */
@Component
@UserDocumentation(title = "Commands for Facts Management",
  description = "These command provide CRUD for facts, associated storages, and fact partitions")
public class LensFactCommands extends LogicalTableCrudCommand<XFactTable> {

  /**
   * Show facts.
   *
   * @return the string
   */
  @CliCommand(value = "show facts",
    help = "display list of fact tables in current database. "
      + "If optional <cube_name> is supplied, only facts belonging to cube <cube_name> will be displayed")
  public String showFacts(
    @CliOption(key = {"", "cube_name"}, mandatory = false, help = "<cube_name>") String cubeName) {
    return showAll(cubeName);
  }

  /**
   * Creates the fact.
   *
   * @param path Path to fact spec
   * @return the string
   */
  @CliCommand(value = "create fact", help = "create a fact table with spec from <path-to-fact-spec-file>")
  public String createFact(
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-fact-spec-file>") @NonNull final File path) {
    return create(path, false);
  }

  /**
   * Describe fact table.
   *
   * @param name the fact
   * @return the string
   */
  @CliCommand(value = "describe fact", help = "describe fact <fact_name>")
  public String describeFactTable(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String name) {
    return describe(name);
  }

  /**
   * Update fact table.
   *
   * @param name     fact name to be updated
   * @param specPath path to the file containing new spec of the fact
   * @return the string
   */
  @CliCommand(value = "update fact", help = "update fact <fact_name> taking spec from <path-to-fact-spec>")
  public String updateFactTable(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String name,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-fact-spec>") @NonNull final File specPath) {
    return update(name, specPath);
  }

  /**
   * Drop fact.
   *
   * @param fact    the fact
   * @param cascade the cascade
   * @return the string
   */
  @CliCommand(value = "drop fact",
    help = "drops fact <fact_name>."
      + " If <cascade> is true, all the storage tables associated with the fact <fact_name> are also dropped."
      + " By default <cascade> is false")
  public String dropFact(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String fact,
    @CliOption(key = {"cascade"}, mandatory = false, unspecifiedDefaultValue = "false", help = "<cascade>")
    boolean cascade) {
    return drop(fact, cascade);
  }

  /**
   * Gets the fact storages.
   *
   * @param tableName the fact
   * @return the fact storages
   */
  @CliCommand(value = "fact list storage", help = "display list of storages associated to fact <fact_name>")
  public String getFactStorages(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName) {
    return showAllStorages(tableName);
  }

  /**
   * Adds the new fact storage.
   *
   * @param tableName the fact name
   * @param path      the path to storage spec
   * @return the string
   */
  @CliCommand(value = "fact add storage",
    help = "adds a new storage to fact <fact_name>, taking storage spec from <path-to-storage-spec>")
  public String addNewFactStorage(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<path-to-storage-spec>") @NonNull final File path) {
    return addStorage(tableName, path);
  }

  /**
   * Gets the storage from fact.
   *
   * @param tableName fact table name
   * @param storage      storage spec path
   * @return the storage from fact
   */
  @CliCommand(value = "fact get storage", help = "describe storage <storage_name> of fact <fact_name>")
  public String getStorageFromFact(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<path-to-storage-spec>") String storage) {
    return getStorage(tableName, storage);
  }

  /**
   * Drop storage from fact.
   *
   * @param tableName   the table name
   * @param storageName the storage name
   * @return the string
   */
  @CliCommand(value = "fact drop storage", help = "drop storage <storage_name> from fact <fact_name>")
  public String dropStorageFromFact(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName) {
    return dropStorage(tableName, storageName);
  }

  /**
   * Drop all fact storages.
   *
   * @param name the table
   * @return the string
   */
  @CliCommand(value = "fact drop all storages", help = "drop all storages associated to fact <fact_name>")
  public String dropAllFactStorages(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String name) {
    return dropAllStorages(name);
  }

  /**
   * Gets the all partitions of fact.
   *
   * @param tableName   fact name
   * @param storageName storage name
   * @param filter      partition filter
   * @return the all partitions of fact
   */
  @CliCommand(value = "fact list partitions",
    help = "get all partitions associated with fact <fact_name>, storage <storage_name> filtered by <partition-filter>")
  public String getAllPartitionsOfFact(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "filter"}, mandatory = false, help = "<partition-filter>") String filter) {
    return getAllPartitions(tableName, storageName, filter);
  }

  /**
   * Drop all partitions of fact.
   *
   * @param tableName   fact name
   * @param storageName storage name
   * @param filter      partition query filter
   * @return the string
   */
  @CliCommand(value = "fact drop partitions",
    help = "drop all partitions associated with fact <fact_name>, "
      + "storage <storage_name> filtered by <partition-filter>")
  public String dropAllPartitionsOfFact(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "filter"}, mandatory = false, help = "<partition-filter>") String filter) {
    return dropPartitions(tableName, storageName, filter);
  }

  /**
   * Adds the partition to fact.
   *
   * @param tableName   fact name
   * @param storageName storage name
   * @param path        partition spec path
   * @return the string
   */
  @CliCommand(value = "fact add single-partition",
    help = "add single partition to fact <fact_name>'s"
      + " storage <storage_name>, reading spec from <partition-spec-path>")
  public String addPartitionToFact(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<partition-spec-path>") @NonNull final File path) {
    return addPartition(tableName, storageName, path);
  }
  /**
   * updates the partition in the fact.
   *
   * @param tableName   fact name
   * @param storageName storage name
   * @param path        partition spec path
   * @return the string
   */
  @CliCommand(value = "fact update single-partition",
    help = "update single partition to fact <fact_name>'s"
      + " storage <storage_name>, reading spec from <partition-spec-path>"
      + " The partition has to exist to be eligible for updation.")
  public String updatePartitionOfFact(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<partition-spec-path>") @NonNull final File path) {
    return updatePartition(tableName, storageName, path);
  }

  @CliCommand(value = "fact add partitions",
    help = "add multiple partition to fact <fact_name>'s"
      + " storage <storage_name>, reading partition list spec from <partition-list-spec-path>")
  public String addPartitionsToFact(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<partition-list-spec-path>") @NonNull final File path) {
    return addPartitions(tableName, storageName, path.getPath());
  }

  @CliCommand(value = "fact update partitions",
    help = "update multiple partition of fact <fact_name>'s"
      + " storage <storage_name>, reading partition list spec from <partition-list-spec-path>"
      +" The partitions have to exist to be eligible for updation.")
  public String updatePartitionsOfFact(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String tableName,
    @CliOption(key = {"", "storage_name"}, mandatory = true, help = "<storage_name>") String storageName,
    @CliOption(key = {"", "path"}, mandatory = true, help = "<partition-list-spec-path>") String path) {
    return updatePartitions(tableName, storageName, path);
  }

  @CliCommand(value = "fact timelines",
    help = "get timelines for fact. Can optionally specify storage, update period and time dimension to filter by."
      + " Instead of time dimension, partition column can be directly passed as <time_dimension>")
  public List<String> getTimelines(
    @CliOption(key = {"", "fact_name"}, mandatory = true, help = "<fact_name>") String factName,
    @CliOption(key = {"storage_name"}, mandatory = false, help = "<storage_name>") String storageName,
    @CliOption(key = {"update_period"}, mandatory = false, help = "<update_period>") String updatePeriod,
    @CliOption(key = {"time_dimension"}, mandatory = false, help = "<time_dimension>") String timeDimension) {
    return getClient().getPartitionTimelines(factName, storageName, updatePeriod, timeDimension);
  }

  @Override
  protected XStorageTableElement readStorage(String tableName, String storage) {
    return getClient().getStorageFromFact(tableName, storage);
  }

  @Override
  public APIResult doDropStorage(String tableName, String storageName) {
    return getClient().dropStorageFromFact(tableName, storageName);
  }

  @Override
  public List<String> getAllStorages(String name) {
    return getClient().getFactStorages(name);
  }

  @Override
  public APIResult doAddStorage(String name, String path) {
    return getClient().addStorageToFact(name, path);
  }

  @Override
  public APIResult doDropAllStorages(String name) {
    return getClient().dropAllStoragesOfFact(name);
  }

  @Override
  protected List<XPartition> readAllPartitions(String tableName, String storageName, String filter) {
    return getClient().getAllPartitionsOfFact(tableName, storageName, filter);
  }

  @Override
  protected APIResult doAddPartition(String tableName, String storageName, String path) {
    return getClient().addPartitionToFact(tableName, storageName, path);
  }

  @Override
  protected APIResult doAddPartitions(String tableName, String storageName, String path) {
    return getClient().addPartitionsToFact(tableName, storageName, path);
  }

  @Override
  protected APIResult doDropPartitions(String tableName, String storageName, String filter) {
    return getClient().dropAllPartitionsOfFact(tableName, storageName, filter);
  }

  @Override
  protected APIResult doUpdatePartition(String tableName, String storageName, String validPath) {
    return getClient().updatePartitionOfFact(tableName, storageName, validPath);
  }

  @Override
  protected APIResult doUpdatePartitions(String tableName, String storageName, String validPath) {
    return getClient().updatePartitionsOfFact(tableName, storageName, validPath);
  }

  @Override
  public List<String> getAll() {
    return getClient().getAllFactTables();
  }

  @Override
  public List<String> getAll(String cubeName) {
    return getClient().getAllFactTables(cubeName);
  }

  @Override
  protected APIResult doCreate(String path, boolean ignoreIfExists) {
    return getClient().createFactTable(path);
  }

  @Override
  protected XFactTable doRead(String name) {
    return getClient().getFactTable(name);
  }

  @Override
  public APIResult doUpdate(String name, String path) {
    return getClient().updateFactTable(name, path);
  }

  @Override
  protected APIResult doDelete(String name, boolean cascade) {
    return getClient().dropFactTable(name, cascade);
  }
}
