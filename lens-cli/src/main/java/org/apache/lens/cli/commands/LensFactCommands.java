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
 * The Class LensFactCommands.
 */
@Component
public class LensFactCommands extends BaseLensCommand implements CommandMarker {

  /**
   * Show facts.
   *
   * @return the string
   */
  @CliCommand(value = "show facts", help = "display list of fact tables in database")
  public String showFacts() {
    List<String> facts = getClient().getAllFactTables();
    if (facts != null) {
      return Joiner.on("\n").join(facts);
    } else {
      return "No Facts Found";
    }
  }

  /**
   * Creates the fact.
   *
   * @param tableFilePair
   *          the table file pair
   * @return the string
   */
  @CliCommand(value = "create fact", help = "create a fact table")
  public String createFact(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<fact spec path> <storage spec path>") String tableFilePair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(tableFilePair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " + "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[0]);

    if (!f.exists()) {
      return "Fact spec path" + f.getAbsolutePath() + " does not exist. Please check the path";
    }
    f = new File(pair[1]);
    if (!f.exists()) {
      return "Storage spech path " + f.getAbsolutePath() + " does not exist. Please check the path";
    }

    APIResult result = getClient().createFactTable(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Fact table Successfully completed";
    } else {
      return "Fact table creation failed";
    }
  }

  /**
   * Drop fact.
   *
   * @param fact
   *          the fact
   * @param cascade
   *          the cascade
   * @return the string
   */
  @CliCommand(value = "drop fact", help = "drop fact table")
  public String dropFact(
      @CliOption(key = { "", "table" }, mandatory = true, help = "table name to be dropped") String fact,
      @CliOption(key = { "cascade" }, mandatory = false, unspecifiedDefaultValue = "false") boolean cascade) {
    APIResult result = getClient().dropFactTable(fact, cascade);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully dropped " + fact + "!!!";
    } else {
      return "Dropping " + fact + " table failed";
    }
  }

  /**
   * Update fact table.
   *
   * @param specPair
   *          the spec pair
   * @return the string
   */
  @CliCommand(value = "update fact", help = "update fact table")
  public String updateFactTable(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<table-name> <path to table-spec>") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " + "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[1]);

    if (!f.exists()) {
      return "Fact spec path" + f.getAbsolutePath() + " does not exist. Please check the path";
    }

    APIResult result = getClient().updateFactTable(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Update of " + pair[0] + " succeeded";
    } else {
      return "Update of " + pair[0] + " failed";
    }
  }

  /**
   * Describe fact table.
   *
   * @param fact
   *          the fact
   * @return the string
   */
  @CliCommand(value = "describe fact", help = "describe a fact table")
  public String describeFactTable(
      @CliOption(key = { "", "table" }, mandatory = true, help = "tablename to be described") String fact) {
    try {
      return formatJson(mapper.writer(pp).writeValueAsString(getClient().getFactTable(fact)));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Gets the fact storages.
   *
   * @param fact
   *          the fact
   * @return the fact storages
   */
  @CliCommand(value = "fact list storage", help = "display list of storages associated to fact table")
  public String getFactStorages(
      @CliOption(key = { "", "table" }, mandatory = true, help = "tablename for getting storages") String fact) {
    List<String> storages = getClient().getFactStorages(fact);
    if (storages == null || storages.isEmpty()) {
      return "No storages found for " + fact;
    }
    return Joiner.on("\n").join(storages);
  }

  /**
   * Drop all fact storages.
   *
   * @param table
   *          the table
   * @return the string
   */
  @CliCommand(value = "fact dropall storages", help = "drop all storages associated to fact table")
  public String dropAllFactStorages(
      @CliOption(key = { "", "table" }, mandatory = true, help = "tablename for dropping all storages") String table) {
    APIResult result = getClient().dropAllStoragesOfFact(table);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "All storages of " + table + " dropped successfully";
    } else {
      return "Error dropping storages of " + table;
    }
  }

  /**
   * Adds the new fact storage.
   *
   * @param tablepair
   *          the tablepair
   * @return the string
   */
  @CliCommand(value = "fact add storage", help = "adds a new storage to fact")
  public String addNewFactStorage(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<table> <path to storage-spec>") String tablepair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " + "format. fact add storage <table> <storage spec path>";
    }

    File f = new File(pair[1]);
    if (!f.exists()) {
      return "Storage spech path " + f.getAbsolutePath() + " does not exist. Please check the path";
    }

    APIResult result = getClient().addStorageToFact(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Fact table storage addition completed";
    } else {
      return "Fact table storage addition failed";
    }
  }

  /**
   * Drop storage from fact.
   *
   * @param tablepair
   *          the tablepair
   * @return the string
   */
  @CliCommand(value = "fact drop storage", help = "drop a storage from fact")
  public String dropStorageFromFact(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<table-name> <storage-name>") String tablepair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " + "format. fact drop storage <table> <storage>";
    }

    APIResult result = getClient().dropStorageFromFact(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Fact table storage removal successful";
    } else {
      return "Fact table storage removal failed";
    }
  }

  /**
   * Gets the storage from fact.
   *
   * @param tablepair
   *          the tablepair
   * @return the storage from fact
   */
  @CliCommand(value = "fact get storage", help = "get storage of fact table")
  public String getStorageFromFact(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<table-name> <storage-name>") String tablepair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " + "format. fact get storage <table> <storage>";
    }
    try {
      return formatJson(mapper.writer(pp).writeValueAsString(getClient().getStorageFromFact(pair[0], pair[1])));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Gets the all partitions of fact.
   *
   * @param specPair
   *          the spec pair
   * @return the all partitions of fact
   */
  @CliCommand(value = "fact list partitions", help = "get all partitions associated with fact")
  public String getAllPartitionsOfFact(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<table-name> <storageName> [optional <partition query filter> to get]") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length == 2) {
      try {
        return formatJson(mapper.writer(pp).writeValueAsString(getClient().getAllPartitionsOfFact(pair[0], pair[1])));
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
    if (pair.length == 3) {
      try {
        return formatJson(mapper.writer(pp).writeValueAsString(
            getClient().getAllPartitionsOfFact(pair[0], pair[1], pair[2])));
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return "Syntax error, please try in following "
    + "format. fact list partitions <table> <storage> [partition values]";
  }

  /**
   * Drop all partitions of fact.
   *
   * @param specPair
   *          the spec pair
   * @return the string
   */
  @CliCommand(value = "fact drop partitions", help = "drop all partitions associated with fact")
  public String dropAllPartitionsOfFact(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<tablename> <storageName> [optional <partition query filter> to drop]") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    APIResult result;
    if (pair.length == 2) {
      result = getClient().dropAllPartitionsOfFact(pair[0], pair[1]);
    }
    if (pair.length == 3) {
      result = getClient().dropAllPartitionsOfFact(pair[0], pair[1], pair[3]);
    } else {
      return "Syntax error, please try in following "
          + "format. fact drop partitions <table> <storage> [partition values]";
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
  @CliCommand(value = "fact add partition", help = "add a partition to fact table")
  public String addPartitionToFact(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<table> <storage> <path to partition spec>") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    APIResult result;
    if (pair.length != 3) {
      return "Syntax error, please try in following " + "format. fact add partition <table> <storage> <partition spec>";
    }

    File f = new File(pair[2]);
    if (!f.exists()) {
      return "Partition spec does not exist";
    }

    result = getClient().addPartitionToFact(pair[0], pair[1], pair[2]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully added partition to " + pair[0];
    } else {
      return "failure in  addition of partition to " + pair[0];
    }
  }

}
