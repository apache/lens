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
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import org.apache.lens.api.APIResult;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * The Class LensDimensionTableCommands.
 */
@Component
public class LensDimensionTableCommands extends BaseLensCommand implements CommandMarker {

  /**
   * Show dimension tables.
   *
   * @return the string
   */
  @CliCommand(value = "show dimtables", help = "show list of dimension tables in database")
  public String showDimensionTables() {
    List<String> dims = getClient().getAllDimensionTables();
    if (dims != null) {
      return Joiner.on("\n").join(dims);
    } else {
      return "No Dimensions Found";
    }
  }

  /**
   * Creates the dimension table.
   *
   * @param dimPair
   *          the dim pair
   * @return the string
   */
  @CliCommand(value = "create dimtable", help = "Create a new dimension table")
  public String createDimensionTable(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<path to dim-spec> <path to storage-spec>") String dimPair) {

    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(dimPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following "
          + "format. create dimtable <dimtable spec path> <storage spec path>";
    }

    File f = new File(pair[0]);
    if (!f.exists()) {
      return "dimtable spec path" + f.getAbsolutePath() + " does not exist. Please check the path";
    }

    f = new File(pair[1]);
    if (!f.exists()) {
      return "storage spec path" + f.getAbsolutePath() + " does not exist. Please check the path";
    }
    APIResult result = getClient().createDimensionTable(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "create dimension table succeeded";
    } else {
      return "create dimension table failed";
    }
  }

  /**
   * Drop dimension table.
   *
   * @param dim
   *          the dim
   * @param cascade
   *          the cascade
   * @return the string
   */
  @CliCommand(value = "drop dimtable", help = "drop dimension table")
  public String dropDimensionTable(
      @CliOption(key = { "", "table" }, mandatory = true, help = "dimension table name to be dropped") String dim,
      @CliOption(key = { "cascade" }, mandatory = false, unspecifiedDefaultValue = "false") boolean cascade) {
    APIResult result = getClient().dropDimensionTable(dim, cascade);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully dropped " + dim + "!!!";
    } else {
      return "Dropping " + dim + " table failed";
    }
  }

  /**
   * Update dimension table.
   *
   * @param specPair
   *          the spec pair
   * @return the string
   */
  @CliCommand(value = "update dimtable", help = "update dimension table")
  public String updateDimensionTable(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<dimension-table-name> <path to table-spec>") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following "
          + "format. create dimtable <dimtable spec path> <storage spec path>";
    }

    File f = new File(pair[1]);

    if (!f.exists()) {
      return "Fact spec path" + f.getAbsolutePath() + " does not exist. Please check the path";
    }

    APIResult result = getClient().updateDimensionTable(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Update of " + pair[0] + " succeeded";
    } else {
      return "Update of " + pair[0] + " failed";
    }
  }

  /**
   * Describe dimension table.
   *
   * @param dim
   *          the dim
   * @return the string
   */
  @CliCommand(value = "describe dimtable", help = "describe a dimension table")
  public String describeDimensionTable(
      @CliOption(key = { "", "table" }, mandatory = true, help = "dimension table name to be described") String dim) {
    try {
      return formatJson(mapper.writer(pp).writeValueAsString(getClient().getDimensionTable(dim)));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Gets the dim storages.
   *
   * @param dim
   *          the dim
   * @return the dim storages
   */
  @CliCommand(value = "dimtable list storage", help = "display list of storage associated to dimension table")
  public String getDimStorages(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<table-name> for listing storages") String dim) {
    List<String> storages = getClient().getDimStorages(dim);
    StringBuilder sb = new StringBuilder();
    for (String storage : storages) {
      if (!storage.isEmpty()) {
        sb.append(storage).append("\n");
      }
    }

    if (sb.toString().isEmpty()) {
      return "No storages found for " + dim;
    }
    return sb.toString().substring(0, sb.toString().length() - 1);
  }

  /**
   * Drop all dim storages.
   *
   * @param table
   *          the table
   * @return the string
   */
  @CliCommand(value = "dimtable drop-all storages", help = "drop all storages associated to dimension table")
  public String dropAllDimStorages(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<table-name> for which all storage should be dropped") String table) {
    APIResult result = getClient().dropAllStoragesOfDim(table);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "All storages of " + table + " dropped successfully";
    } else {
      return "Error dropping storages of " + table;
    }
  }

  /**
   * Adds the new dim storage.
   *
   * @param tablepair
   *          the tablepair
   * @return the string
   */
  @CliCommand(value = "dimtable add storage", help = "adds a new storage to dimension")
  public String addNewDimStorage(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<dim-table-name> <path to storage-spec>") String tablepair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following "
          + "format. create dimtable <dimtable spec path> <storage spec path>";
    }

    File f = new File(pair[1]);
    if (!f.exists()) {
      return "Storage spech path " + f.getAbsolutePath() + " does not exist. Please check the path";
    }

    APIResult result = getClient().addStorageToDim(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Dim table storage addition completed";
    } else {
      return "Dim table storage addition failed";
    }
  }

  /**
   * Drop storage from dim.
   *
   * @param tablepair
   *          the tablepair
   * @return the string
   */
  @CliCommand(value = "dimtable drop storage", help = "drop storage to dimension table")
  public String dropStorageFromDim(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<dimension-table-name> <storage-name>") String tablepair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following "
          + "format. create dimtable <dimtable spec path> <storage spec path>";
    }
    APIResult result = getClient().dropStorageFromDim(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Dim table storage removal successful";
    } else {
      return "Dim table storage removal failed";
    }
  }

  /**
   * Gets the storage from dim.
   *
   * @param tablepair
   *          the tablepair
   * @return the storage from dim
   */
  @CliCommand(value = "dimtable get storage", help = "describe storage of dimension table")
  public String getStorageFromDim(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<dimension-table-name> <storage-name>") String tablepair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following "
          + "format. create dimtable <dimtable spec path> <storage spec path>";
    }
    try {
      return formatJson(mapper.writer(pp).writeValueAsString(getClient().getStorageFromDim(pair[0], pair[1])));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Gets the all partitions of dim.
   *
   * @param specPair
   *          the spec pair
   * @return the all partitions of dim
   */
  @CliCommand(value = "dimtable list partitions", help = "get all partitions associated with dimension table")
  public String getAllPartitionsOfDim(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<dimension-table-name> <storageName> "
          + "[optional <partition query filter> to get]") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length == 2) {
      try {
        return formatJson(mapper.writer(pp).writeValueAsString(getClient().getAllPartitionsOfDim(pair[0], pair[1])));
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    if (pair.length == 3) {
      try {
        return formatJson(mapper.writer(pp).writeValueAsString(
            getClient().getAllPartitionsOfDim(pair[0], pair[1], pair[2])));
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    return "Syntax error, please try in following "
    + "format. dim list partitions <table> <storage> [partition values]";
  }

  /**
   * Drop all partitions of dim.
   *
   * @param specPair
   *          the spec pair
   * @return the string
   */
  @CliCommand(value = "dimtable drop partitions", help = "drop all partitions associated with dimension table")
  public String dropAllPartitionsOfDim(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<dimension-table-name> <storageName> "
          + "[optional <partition query filter> to drop]") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    APIResult result;
    if (pair.length == 2) {
      result = getClient().dropAllPartitionsOfDim(pair[0], pair[1]);
    }
    if (pair.length == 3) {
      result = getClient().dropAllPartitionsOfDim(pair[0], pair[1], pair[3]);
    } else {
      return "Syntax error, please try in following "
          + "format. dimtable drop partitions <table> <storage> [partition values]";
    }

    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully dropped partition of " + pair[0];
    } else {
      return "failure in  dropping partition of " + pair[0];
    }

  }

  /**
   * Adds the partition to fact.
   *
   * @param specPair
   *          the spec pair
   * @return the string
   */
  @CliCommand(value = "dimtable add partition", help = "add a partition to dim table")
  public String addPartitionToFact(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<dimension-table-name> <storage-name>"
          + " <path to partition specification>") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    APIResult result;
    if (pair.length != 3) {
      return "Syntax error, please try in following "
          + "format. dimtable add partition <table> <storage> <partition spec>";
    }

    File f = new File(pair[2]);
    if (!f.exists()) {
      return "Partition spec does not exist";
    }

    result = getClient().addPartitionToDim(pair[0], pair[1], pair[2]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully added partition to " + pair[0];
    } else {
      return "failure in  addition of partition to " + pair[0];
    }
  }

}
