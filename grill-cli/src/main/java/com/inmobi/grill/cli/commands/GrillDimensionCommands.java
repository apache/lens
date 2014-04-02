package com.inmobi.grill.cli.commands;

/*
 * #%L
 * Grill CLI
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.metastore.*;
import com.inmobi.grill.client.GrillClient;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;

@Component
public class GrillDimensionCommands implements CommandMarker {
  private GrillClient client;


  public void setClient(GrillClient client) {
    this.client = client;
  }

  @CliCommand(value = "show dimensions",
      help = "show list of dimension tables in database")
  public String showDimensions() {
    List<String> dims = client.getAllDimensionTables();
    return Joiner.on("\n").join(dims);
  }

  @CliCommand(value = "create dimension", help = "Create a new dimension table")
  public String createDimension(@CliOption(key = {"", "dimension"},
      mandatory = true, help = "<dim-spec> <storage-spec>") String dimPair) {

    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(dimPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[0]);
    if (!f.exists()) {
      return "dim spec path"
          + f.getAbsolutePath()
          + " does not exist. Please check the path";
    }

    f = new File(pair[1]);
    if (!f.exists()) {
      return "storage spec path"
          + f.getAbsolutePath()
          + " does not exist. Please check the path";
    }
    APIResult result = client.createDimension(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "create dimension table succeeded";
    } else {
      return "create dimension table failed";
    }
  }


  @CliCommand(value = "drop dimension", help = "drop dimension table")
  public String dropDimensionTable(@CliOption(key = {"", "table"},
      mandatory = true, help = "table name to be dropped") String dim,
                                   @CliOption(key = {"cascade"}, mandatory = false,
                                       unspecifiedDefaultValue = "false") boolean cascade) {
    APIResult result = client.dropDimensionTable(dim, cascade);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully dropped " + dim + "!!!";
    } else {
      return "Dropping " + dim + " table failed";
    }
  }

  @CliCommand(value = "update dimension", help = "update dimension table")
  public String updateDimensionTable(@CliOption(key = {"", "table"}, mandatory = true, help = "<table-name> <table-spec>") String specPair) {
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[1]);

    if (!f.exists()) {
      return "Fact spec path"
          + f.getAbsolutePath()
          + " does not exist. Please check the path";
    }

    APIResult result = client.updateDimensionTable(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Update of " + pair[0] + " succeeded";
    } else {
      return "Update of " + pair[0] + " failed";
    }
  }


  @CliCommand(value = "describe dimension", help = "describe a fact table")
  public String describeDimensionTable(@CliOption(key = {"", "table"},
      mandatory = true, help = "table name to be described") String dim) {
    DimensionTable table = client.getDimensionTable(dim);
    StringBuilder buf = new StringBuilder();

    buf.append("Table Name : ").append(table.getName()).append("\n");
    if(table.getColumns()!= null) {
      buf.append("Columns: ").append("\n");
      buf.append("\t")
          .append("NAME")
          .append("\t")
          .append("TYPE")
          .append("\t")
          .append("COMMENTS")
          .append("\n");
      for (Column col : table.getColumns().getColumns()) {
        buf.append("\t")
            .append(col.getName() != null ? col.getName() : "")
            .append("\t")
            .append(col.getType() != null ? col.getType() : "")
            .append("\t")
            .append(col.getComment() != null ? col.getComment() : "")
            .append("\n");
      }
    }
    if(table.getProperties()!= null) {
     buf.append(FormatUtils.formatProperties(table.getProperties().getProperties()));
    }
    if(table.getDimensionsReferences() != null ) {
      buf.append("References : ").append("\n");
      buf.append("\t").append("reference column name")
          .append("\t")
          .append("list of table reference <table>.<col>")
          .append("\n");
      for (DimensionReference ref : table.getDimensionsReferences().getDimReferences()) {
        buf.append("\t")
            .append(ref.getDimensionColumn())
            .append("\t")
            .append(getXtableString(ref.getTableReferences())).append("\n");
      }
    }
    if(table.getStorageDumpPeriods() != null) {
      buf.append("Update Period : ").append("\n");
      buf.append("\t").append("storage name").append("\t")
          .append("list of update period").append("\n");
      for (UpdatePeriodElement element : table.getStorageDumpPeriods().getUpdatePeriodElement()) {
        buf.append("\t")
            .append(element.getStorageName())
            .append("\t")
            .append(Joiner.on(",").skipNulls().join(element.getUpdatePeriods()))
            .append("\n");
      }
    }
    return buf.toString();
  }

  private String getXtableString(List<XTablereference> tableReferences) {
    StringBuilder builder = new StringBuilder();
    for (XTablereference ref : tableReferences) {
      builder.append(ref.getDestTable())
          .append(".")
          .append(ref.getDestColumn())
          .append(",");
    }
    return builder.toString();
  }


  @CliCommand(value = "dim list storage",
      help = "display list of storage associated to dimension table")
  public String getDimStorages(@CliOption(key = {"", "table"},
      mandatory = true, help = "table name to be dropped") String fact){
    List<String> storages = client.getDimStorages(fact);
    return Joiner.on("\n").join(storages);
  }

  @CliCommand(value = "dim drop-all storages",
      help = "drop all storages associated to dimension table")
  public String dropAllDimStorages(@CliOption(key = {"", "table"},
      mandatory = true, help = "table name to be dropped") String table){
    APIResult result = client.dropAllStoragesOfDim(table);
    if(result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "All storages of " + table + " dropped successfully";
    } else {
      return "Error dropping storages of " + table ;
    }
  }


  @CliCommand(value = "dim add storage", help = "adds a new storage to dimension")
  public String addNewDimStorage(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table> <storage>") String tablepair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[0]);

    if (!f.exists()) {
      return "Fact spec path"
          + f.getAbsolutePath()
          + " does not exist. Please check the path";
    }
    f = new File(pair[1]);
    if (!f.exists()) {
      return "Storage spech path "
          + f.getAbsolutePath() +
          " does not exist. Please check the path";
    }

    APIResult result = client.addStorageToDim(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Dim table storage addition completed";
    } else {
      return "Dim table storage addition failed";
    }
  }



  @CliCommand(value = "dim drop storage", help = "drop storage to dimension table")
  public String dropStorageFromDim(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table> <storage-spec>") String tablepair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[0]);

    if (!f.exists()) {
      return "Fact spec path"
          + f.getAbsolutePath()
          + " does not exist. Please check the path";
    }
    f = new File(pair[1]);
    if (!f.exists()) {
      return "Storage spech path "
          + f.getAbsolutePath() +
          " does not exist. Please check the path";
    }

    APIResult result = client.dropStorageFromDim(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Dim table storage removal successful";
    } else {
      return "Dim table storage removal failed";
    }
  }


  @CliCommand(value = "dim get storage", help = "describe storage of dimension table")
  public String getStorageFromDim(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table-name> <storage-spec>") String tablepair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. create fact <fact spec path> <storage spec path>";
    }

    XStorageTableElement element = client.getStorageFromDim(pair[0], pair[1]);
    return FormatUtils.getStorageString(element);
  }

  @CliCommand(value = "dim list partitions",
      help = "get all partitions associated with dimension table")
  public String getAllPartitionsOfDim(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table> <storageName> [<part-vals]") String specPair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length == 2) {
      List<XPartition> partitions = client.getAllPartitionsOfDim(pair[0],pair[1]);
      return FormatUtils.formatPartitions(partitions);
    }

    if(pair.length == 3) {
      List<XPartition> partitions = client.getAllPartitionsOfDim(pair[0],pair[1], pair[2]);
      return FormatUtils.formatPartitions(partitions);
    }

    return "Syntax error, please try in following " +
        "format. dim list partitions <table> <storage> [partition values]";
  }


  @CliCommand(value = "dim drop partitions",
      help = "drop all partitions associated with dimension table")
  public String dropAllPartitionsOfDim(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table> <storageName> [<list>]") String specPair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    APIResult result;
    if(pair.length == 2) {
      result = client.dropAllPartitionsOfDim(pair[0], pair[1]);
    }
    if (pair.length == 3) {
      result = client.dropAllPartitionsOfDim(pair[0], pair[1], pair[3]);
    } else {
      return "Syntax error, please try in following " +
          "format. fact drop partitions <table> <storage> [partition values]";
    }

    if(result.getStatus() == APIResult.Status.SUCCEEDED ) {
      return "Successfully dropped partition of "  + pair[0];
    } else {
      return "failure in  dropping partition of "  + pair[0];
    }

  }


  @CliCommand(value = "dim add partition", help = "add a partition to dim table")
  public String addPartitionToFact(@CliOption(key = {"","table"},
      mandatory = true, help = "<table> <storage> <partspec>") String specPair) {
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    APIResult result;
    if(pair.length != 3) {
      return "Syntax error, please try in following " +
          "format. fact add partition <table> <storage> <partition spec>";
    }

    File f = new File(pair[2]);
    if(!f.exists()) {
      return "Partition spec does not exist";
    }

    result = client.addPartitionToDim(pair[0], pair[1], pair[2]);
    if(result.getStatus() == APIResult.Status.SUCCEEDED ) {
      return "Successfully added partition to "  + pair[0];
    } else {
      return "failure in  addition of partition to "  + pair[0];
    }
  }

}
