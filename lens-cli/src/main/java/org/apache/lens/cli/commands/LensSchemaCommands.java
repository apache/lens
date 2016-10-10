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

import java.io.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.JLineShellComponent;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.support.logging.HandlerUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.google.common.collect.Lists;

@Component
@UserDocumentation(title = "Creating schema with one command",
  description = "")
public class LensSchemaCommands implements CommandMarker {
  public static final String STRUCTURE = "\n"
    + ".\n"
    + "|-- storages\n"
    + "|  |-- storage1.xml\n"
    + "|  |-- storage2.xml\n"
    + "|\n"
    + "|-- dimensions\n"
    + "|  |-- dim1.xml\n"
    + "|  |-- dim2.xml\n"
    + "|\n"
    + "|-- cubes\n"
    + "|  |-- base\n"
    + "|  |  |-- base_cube1.xml\n"
    + "|  |  |-- base_cube2.xml\n"
    + "|  |\n"
    + "|  |-- derived\n"
    + "|  |  |-- derived_cube1.xml\n"
    + "|  |  |-- derived_cube2.xml\n"
    + "|  |\n"
    + "|  |-- independent_cube1.xml\n"
    + "|  |-- independent_cube2.xml\n"
    + "|\n"
    + "|-- dimensiontables\n"
    + "|  |-- dimtable1.xml\n"
    + "|  |-- dimtable2.xml\n"
    + "|\n"
    + "|-- dimtables\n"
    + "|  |-- dimtable3.xml\n"
    + "|  |-- dimtable4.xml\n"
    + "|\n"
    + "|-- facts\n"
    + "   |-- fact1.xml\n"
    + "   |-- fact2.xml\n\n\n"
    + "If your cubes are divided between base and derived cubes,\nit makes sense to seperate into two directories, "
    + "since derived cubes can't be created unless base cube exists.\nIn the other case you can keep them in the cubes "
    + "directory itself.\nFor dimtables, you can keep your schema files in a directory named either dimtables or "
    + "dimensiontables.\nEach of these directories is optional and the order of processing is top to bottom.\nCLI will "
    + "let you know in case of any errors and proceed further without failing in between.";
  protected final Logger logger = HandlerUtils.getLogger(getClass());

  {
    logger.setLevel(Level.FINE);
  }

  private static final FilenameFilter XML_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(".xml");
    }
  };
  @Autowired
  private JLineShellComponent shell;

  @CliCommand(value = {"schema", "create schema"},
    help = "Parses the specified resource file and executes commands for "
      + "creation/updation of schema\nExpected structure is " + STRUCTURE)
  public void script(
    @CliOption(key = {"", "db"},
      help = "<database-to-create-schema-in>", mandatory = true) final String database,
    @CliOption(key = {"", "file", "path"},
      help = "<schema-directory>", mandatory = true) final File schemaDirectory) {
    if (!schemaDirectory.isDirectory()) {
      throw new IllegalStateException("Schema directory should be a directory");
    }

    // ignore result. it can fail if database already exists
    shell.executeCommand("create database " + database);
    if (shell.executeScriptLine("use " + database)) {
      createOrUpdate(new File(schemaDirectory, "storages"), "storage",
        "create storage --path %s", "update storage --name %s --path %s");
      createOrUpdate(new File(schemaDirectory, "dimensions"), "dimension",
        "create dimension --path %s", "update dimension --name %s --path %s");
      createOrUpdate(new File(new File(schemaDirectory, "cubes"), "base"), "base cube",
        "create cube --path %s", "update cube --name %s --path %s");
      createOrUpdate(new File(new File(schemaDirectory, "cubes"), "derived"), "derived cube",
        "create cube --path %s", "update cube --name %s --path %s");
      createOrUpdate(new File(schemaDirectory, "dimensiontables"), "dimension table",
        "create dimtable --path %s", "update dimtable --dimtable_name %s --path %s");
      createOrUpdate(new File(schemaDirectory, "dimtables"), "dimension table",
        "create dimtable --path %s", "update dimtable --dimtable_name %s --path %s");
      createOrUpdate(new File(schemaDirectory, "facts"), "fact",
        "create fact --path %s", "update fact --fact_name %s --path %s");
      createOrUpdate(new File(schemaDirectory, "segmentations"), "fact",
        "create segmentation --path %s", "update segmentation --name %s --path %s");
    } else {
      throw new IllegalStateException("Switching to database " + database + " failed");
    }
  }

  public List<File> createOrUpdate(File parent, String entityType, String createSyntax, String updateSyntax) {
    List<File> failedFiles = Lists.newArrayList();
    // Create/update entities
    if (parent.exists()) {
      Assert.isTrue(parent.isDirectory(), parent.toString() + " must be a directory");
      for (File entityFile : parent.listFiles(XML_FILTER)) {
        String entityName = entityFile.getName().substring(0, entityFile.getName().length() - 4);
        String entityPath = entityFile.getAbsolutePath();
        String createCommand = String.format(createSyntax, entityPath);
        logger.fine(createCommand);
        if (shell.executeScriptLine(createCommand)) {
          logger.info("Created " + entityType + " " + entityName);
        } else {
          logger.warning("Create failed, trying update");
          String updateCommand = String.format(updateSyntax, entityName, entityPath);
          logger.fine(updateCommand);
          if (shell.executeScriptLine(updateCommand)) {
            logger.info("Updated " + entityType + " " + entityName);
          } else {
            logger.severe("Couldn't create or update " + entityType + " " + entityName);
            failedFiles.add(entityFile);
          }
        }
      }
    }
    if (!failedFiles.isEmpty()) {
      logger.severe("Failed for " + entityType + ": " + failedFiles);
    }
    return failedFiles;
  }
}
