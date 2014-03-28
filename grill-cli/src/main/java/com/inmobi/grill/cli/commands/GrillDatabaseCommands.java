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
public class GrillDatabaseCommands implements CommandMarker {

  private GrillClient client;


  public void setClient(GrillClient client) {
    this.client = client;
  }


  @CliCommand(value = "show databases",
      help = "displays list of all databases")
  public String showAllDatabases() {
    List<String> databases = client.getAllDatabases();
    return Joiner.on("\n").join(databases);
  }

  @CliCommand(value = "use",
      help = "change to new database")
  public String switchDatabase(@CliOption(key = {"", "db"},
      mandatory = true,
      help = "Database to change to") String database) {
    boolean status = client.setDatabase(database);
    if (status) {
      return "Successfully switched to " + database;
    } else {
      return "Failed to switch to " + database;
    }
  }


  @CliCommand(value = "create database")
  public String createDatabase(
      @CliOption(key = {"", "db"}, mandatory = true, help = "Database to create")
      String database,
      @CliOption(key = {"ignore"}, mandatory = false,
          unspecifiedDefaultValue = "false")
      boolean ignore) {
    APIResult result = client.createDatabase(database, ignore);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return ("Create database " + database + " successful");
    } else {
      return result.getMessage();
    }
  }

  @CliCommand(value = "drop database")
  public String dropDatabase(
      @CliOption(key = {"", "db"}, mandatory = true, help = "Database to drop")
      String database) {
    APIResult result = client.dropDatabase(database);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return ("drop database " + database + " successful");
    } else {
      return result.getMessage();
    }
  }






}
