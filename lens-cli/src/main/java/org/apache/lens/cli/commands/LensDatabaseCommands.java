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

import org.apache.lens.api.APIResult;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The Class LensDatabaseCommands.
 */
@Component
public class LensDatabaseCommands extends BaseLensCommand implements CommandMarker {

  /**
   * Show all databases.
   *
   * @return the string
   */
  @CliCommand(value = "show databases", help = "displays list of all databases")
  public String showAllDatabases() {
    List<String> databases = getClient().getAllDatabases();
    if (databases != null) {
      return Joiner.on("\n").join(databases);
    } else {
      return "No Dabases found";
    }
  }

  /**
   * Switch database.
   *
   * @param database
   *          the database
   * @return the string
   */
  @CliCommand(value = "use", help = "change to new database")
  public String switchDatabase(
      @CliOption(key = { "", "db" }, mandatory = true, help = "Database to change to") String database) {
    boolean status = getClient().setDatabase(database);
    if (status) {
      return "Successfully switched to " + database;
    } else {
      return "Failed to switch to " + database;
    }
  }

  /**
   * Creates the database.
   *
   * @param database
   *          the database
   * @param ignore
   *          the ignore
   * @return the string
   */
  @CliCommand(value = "create database", help = "create a database with specified name")
  public String createDatabase(
      @CliOption(key = { "", "db" }, mandatory = true, help = "Database to create") String database,
      @CliOption(key = { "ignore" }, mandatory = false, unspecifiedDefaultValue = "false") boolean ignore) {
    APIResult result = getClient().createDatabase(database, ignore);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return ("Create database " + database + " successful");
    } else {
      return result.getMessage();
    }
  }

  /**
   * Drop database.
   *
   * @param database
   *          the database
   * @return the string
   */
  @CliCommand(value = "drop database", help = "drop a database with specified name")
  public String dropDatabase(@CliOption(key = { "", "db" }, mandatory = true, help = "Database to drop") String database) {
    APIResult result = getClient().dropDatabase(database);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return ("drop database " + database + " successful");
    } else {
      return result.getMessage();
    }
  }

}
