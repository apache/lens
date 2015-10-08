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

import java.util.List;

import org.apache.lens.api.APIResult;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * The Class LensDatabaseCommands.
 */
@Component
@UserDocumentation(title = "Commands for Database Management",
  description = "These commands provide CRUD for databases")
public class LensDatabaseCommands extends LensCRUDCommand {

  /**
   * Show all databases.
   *
   * @return the string
   */
  @CliCommand(value = "show databases", help = "displays list of all databases")
  public String showAllDatabases() {
    return showAll();
  }

  /**
   * Switch database.
   *
   * @param database the database
   * @return the string
   */
  @CliCommand(value = "use", help = "change to new database")
  public String switchDatabase(
    @CliOption(key = {"", "db"}, mandatory = true, help = "<database-name>") String database) {
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
   * @param database       the database
   * @param ignoreIfExists the ignore
   * @return the string
   */
  @CliCommand(value = "create database",
    help = "create a database with specified name. if <ignore-if-exists> is true, "
      + "create will not be tried if already exists. Default is false")
  public String createDatabase(
    @CliOption(key = {"", "db"}, mandatory = true, help = "<database-name>") String database,
    @CliOption(key = {"ignoreIfExists"}, mandatory = false, unspecifiedDefaultValue = "false",
      help = "<ignore-if-exists>") boolean ignoreIfExists) {
    // first arg is not file. LensCRUDCommand.create expects file path as first arg. So calling method directly here.
    return doCreate(database, ignoreIfExists).toString().toLowerCase();
  }

  /**
   * Drop database.
   *
   * @param database the database
   * @return the string
   */
  @CliCommand(value = "drop database", help = "drop a database with specified name")
  public String dropDatabase(@CliOption(key = {"", "db"}, mandatory = true, help = "<database-name>") String database,
    @CliOption(key = "cascade", specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean cascade) {
    return drop(database, cascade);
  }

  @Override
  public List<String> getAll() {
    return getClient().getAllDatabases();
  }

  // Create is directly implemented
  @Override
  protected APIResult doCreate(String database, boolean ignoreIfExists) {
    return getClient().createDatabase(database, ignoreIfExists);
  }

  // Doesn't make sense for database
  @Override
  protected Object doRead(String name) {
    return null;
  }

  // Also, doesn't make sense
  @Override
  public APIResult doUpdate(String name, String path) {
    return null;
  }

  @Override
  protected APIResult doDelete(String name, boolean cascade) {
    return getClient().dropDatabase(name, cascade);
  }
}
