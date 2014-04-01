package com.inmobi.grill.cli.commands;

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
public class GrillFactCommands implements CommandMarker {
  private GrillClient client;


  public void setClient(GrillClient client) {
    this.client = client;
  }


  @CliCommand(value = "show facts",
      help = "display list of fact tables in database")
  public String showFacts() {
    List<String> facts = client.getAllFactTables();
    return Joiner.on("\n").join(facts);
  }

  @CliCommand(value = "create fact", help = "create a fact table")
  public String createFact(
      @CliOption(key = {"", "table"},
          mandatory = true,
          help = "<fact spec path> <storage spec path>")
      String tableFilePair) {
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(tableFilePair);
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

    APIResult result = client.createFactTable(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Fact table Successfully completed";
    } else {
      return "Fact table creation failed";
    }
  }

  @CliCommand(value = "drop fact", help = "drop fact table")
  public String dropFact(@CliOption(key = {"", "table"},
      mandatory = true, help = "table name to be dropped") String fact,
                         @CliOption(key = {"cascade"}, mandatory = false,
                             unspecifiedDefaultValue = "false") boolean cascade) {
    APIResult result = client.dropFactTable(fact, cascade);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully dropped " + fact + "!!!";
    } else {
      return "Dropping " + fact + " table failed";
    }
  }

  @CliCommand(value = "update fact", help = "update fact table")
  public String updateFactTable(@CliOption(key = {"", "table"}, mandatory = true, help = "<table-name> <table-spec>") String specPair) {
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

    APIResult result = client.updateFactTable(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Update of " + pair[0] + " succeeded";
    } else {
      return "Update of " + pair[0] + " failed";
    }
  }


  @CliCommand(value = "describe fact", help = "describe a fact table")
  public String describeFactTable(@CliOption(key = {"", "table"},
      mandatory = true, help = "table name to be dropped") String fact) {
    FactTable table = client.getFactTable(fact);
    StringBuilder buf = new StringBuilder();

    buf.append("Table Name : ").append(table.getName()).append("\n");
    buf.append("Cube Name: ").append(table.getCubeName()).append("\n");
    buf.append("Columns: ").append("\n").append("\n\n");
    buf.append("\t")
        .append("NAME")
        .append("\t")
        .append("TYPE")
        .append("\t")
        .append("COMMENTS")
        .append("\n");
    for (Column col : table.getColumns().getColumns()) {
      buf.append("\t")
          .append(col.getName())
          .append("\t")
          .append(col.getType())
          .append("\t")
          .append(col.getComment())
          .append("\n");
    }
    buf.append(FormatUtils.formatProperties(table.getProperties().getProperties()));
    buf.append("Update Period : ").append("\n");
    buf.append("\t").append("storage name").append("\t")
        .append("list of update period").append("\n").append("\n\n");
    for (UpdatePeriodElement element : table.getStorageUpdatePeriods().getUpdatePeriodElement()) {
      buf.append("\t")
          .append(element.getStorageName())
          .append("\t")
          .append(Joiner.on(",").skipNulls().join(element.getUpdatePeriods()))
          .append("\n");
    }
    return buf.toString();
  }

  @CliCommand(value = "fact list storage",
      help = "display list of storages associated to fact table")
  public String getFactStorages(@CliOption(key = {"", "table"},
      mandatory = true, help = "table name to be dropped") String fact){
    List<String> storages = client.getFactStorages(fact);
    return Joiner.on("\n").join(storages);
  }

  @CliCommand(value = "fact dropall storages",
      help = "drop all storages associated to fact table")
  public String dropAllFactStorages(@CliOption(key = {"", "table"},
      mandatory = true, help = "table name to be dropped") String table){
    APIResult result = client.dropAllStoragesOfFact(table);
    if(result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "All storages of " + table + " dropped successfully";
    } else {
      return "Error dropping storages of " + table ;
    }
  }

  @CliCommand(value = "fact add storage", help = "adds a new storage to fact")
  public String addNewFactStorage(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table> <storage-spec>") String tablepair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. fact add storage <table> <storage spec path>";
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

    APIResult result = client.addStorageToFact(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Fact table storage addition completed";
    } else {
      return "Fact table storage addition failed";
    }
  }



  @CliCommand(value = "fact drop storage", help = "drop a storage from fact")
  public String dropStorageFromFact(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table> <storage>") String tablepair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. fact drop storage <table> <storage>";
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

    APIResult result = client.dropStorageFromFact(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Fact table storage removal successful";
    } else {
      return "Fact table storage removal failed";
    }
  }


  @CliCommand(value = "fact get storage", help = "get storage of fact table")
  public String getStorageFromFact(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table> <storage>") String tablepair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(tablepair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. fact get storage <table> <storage>";
    }

    XStorageTableElement element = client.getStorageFromFact(pair[0], pair[1]);
    return FormatUtils.getStorageString(element);
  }


  @CliCommand(value = "fact list partitions",
      help = "get all partitions associated with fact")
  public String getAllPartitionsOfFact(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table> <storageName> [<list>]") String specPair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if(pair.length == 2) {
      List<XPartition> partitions = client.getAllPartitionsOfFact(pair[0], pair[1]);
      return FormatUtils.formatPartitions(partitions);
    }
    if (pair.length == 3) {
      List<XPartition> partitions = client.getAllPartitionsOfFact(pair[0], pair[1], pair[3]);
      return FormatUtils.formatPartitions(partitions);
    }
    return "Syntax error, please try in following " +
        "format. fact list partitions <table> <storage> [partition values]";
  }

  @CliCommand(value = "fact drop partitions",
      help = "drop all partitions associated with fact")
  public String dropAllPartitionsOfFact(@CliOption(key = {"", "table"},
      mandatory = true, help = "<table> <storageName> [<list>]") String specPair){
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    APIResult result;
    if(pair.length == 2) {
      result = client.dropAllPartitionsOfFact(pair[0], pair[1]);
    }
    if (pair.length == 3) {
      result = client.dropAllPartitionsOfFact(pair[0], pair[1], pair[3]);
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

  @CliCommand(value = "fact add partition", help = "add a partition to fact table")
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

    result = client.addPartitionToFact(pair[0], pair[1], pair[2]);
    if(result.getStatus() == APIResult.Status.SUCCEEDED ) {
      return "Successfully added partition to "  + pair[0];
    } else {
      return "failure in  addition of partition to "  + pair[0];
    }
  }

}
