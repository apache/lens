/*
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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lens.api.metastore.*;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.JLineShellComponent;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.support.logging.HandlerUtils;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Component
@UserDocumentation(title = "Creating schema with one command",
  description = "")
public class LensSchemaCommands implements CommandMarker {
  private static final String STRUCTURE = "\n"
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
    + "   |-- fact2.xml\n"
    + "|  |\n"
    + "|  |-- virtual\n"
    + "|  |  |-- virtual_fact1.xml\n"
    + "|  |  |-- virtual_fact2.xml\n"
    + "|  |\n\n\n"
    + "If your cubes are divided between base and derived cubes,\nit makes sense to seperate into two directories, "
    + "since derived cubes can't be created unless base cube exists.\nIn the other case you can keep them in the cubes "
    + "directory itself.\nFor dimtables, you can keep your schema files in a directory named either dimtables or "
    + "dimensiontables.\nEach of these directories is optional and the order of processing is top to bottom.\nCLI will "
    + "let you know in case of any errors and proceed further without failing in between.";
  private final Logger logger = HandlerUtils.getLogger(getClass());

  {
    logger.setLevel(Level.FINE);
  }

  private static final Map<Class<?>, String> CREATE_COMMAND_MAP = Maps.newHashMap();
  private static final Map<Class<?>, String> UPDATE_COMMAND_MAP = Maps.newHashMap();

  @Autowired
  private JLineShellComponent shell;

  static {
    CREATE_COMMAND_MAP.put(XStorage.class, "create storage --path %s");
    UPDATE_COMMAND_MAP.put(XStorage.class, "update storage --name %s --path %s");
    CREATE_COMMAND_MAP.put(XDimension.class, "create dimension --path %s");
    UPDATE_COMMAND_MAP.put(XDimension.class, "update dimension --name %s --path %s");
    CREATE_COMMAND_MAP.put(XBaseCube.class, "create cube --path %s");
    UPDATE_COMMAND_MAP.put(XBaseCube.class, "update cube --name %s --path %s");
    CREATE_COMMAND_MAP.put(XDerivedCube.class, "create cube --path %s");
    UPDATE_COMMAND_MAP.put(XDerivedCube.class, "update cube --name %s --path %s");
    CREATE_COMMAND_MAP.put(XDimensionTable.class, "create dimtable --path %s");
    UPDATE_COMMAND_MAP.put(XDimensionTable.class, "update dimtable --dimtable_name %s --path %s");
    CREATE_COMMAND_MAP.put(XDimensionTable.class, "create dimtable --path %s");
    UPDATE_COMMAND_MAP.put(XDimensionTable.class, "update dimtable --dimtable_name %s --path %s");
    CREATE_COMMAND_MAP.put(XFactTable.class, "create fact --path %s");
    UPDATE_COMMAND_MAP.put(XFactTable.class, "update fact --fact_name %s --path %s");
    CREATE_COMMAND_MAP.put(XVirtualFactTable.class, "create fact --path %s");
    UPDATE_COMMAND_MAP.put(XVirtualFactTable.class, "update fact --fact_name %s --path %s");
    CREATE_COMMAND_MAP.put(XSegmentation.class, "create segmentation --path %s");
    UPDATE_COMMAND_MAP.put(XSegmentation.class, "update segmentation --name %s --path %s");
  }

  private final SchemaCreateUpdateCommandRunner processor = new SchemaCreateUpdateCommandRunner();

  @CliCommand(value = {"schema", "create schema"},
    help = "Parses the specified resource file and executes commands for "
      + "creation/updation of schema. If <schema-type-filter> is provided, only schema types matching that will "
      + "be worked upon. If <file-name-filter> is provided, then only those files that contain the filter value "
      + "will be worked upon. \nExpected directory structure is " + STRUCTURE)
  public void script(
    @CliOption(key = {"", "db"},
      help = "<database-to-create-schema-in>", mandatory = true) final String database,
    @CliOption(key = {"", "file", "path"},
      help = "<schema-directory>", mandatory = true) final File schemaDirectory,
    @CliOption(key = {"", "type"},
      help = "<schema-type-filter>") final String type,
    @CliOption(key = {"", "name"},
      help = "<file-name-filter>") final String name) {
    if (!schemaDirectory.isDirectory()) {
      throw new IllegalStateException("Schema directory should be a directory");
    }

    // ignore result. it can fail if database already exists
    shell.executeCommand("create database " + database);
    if (shell.executeScriptLine("use " + database)) {
      SchemaTraverser schemaTraverser = new SchemaTraverser(schemaDirectory, processor, type, name);
      schemaTraverser.run();
      logger.info("Finished all create/update commands");
      logger.severe("All failures: " + processor.failedFor);
    } else {
      throw new IllegalStateException("Switching to database " + database + " failed");
    }
  }

  private class SchemaCreateUpdateCommandRunner implements SchemaTraverser.SchemaEntityProcessor {
    List<String> failedFor = Lists.newArrayList();

    @Override
    public void accept(File entityFile, Class<?> type) {
      String entityName = entityFile.getName().substring(0, entityFile.getName().length() - 4);
      String entityPath = entityFile.getAbsolutePath();
      String createCommand = String.format(CREATE_COMMAND_MAP.get(type), entityPath);
      String entityType = createCommand.substring(7, createCommand.indexOf(" ", 9));
      logger.fine(createCommand);
      if (shell.executeScriptLine(createCommand)) {
        logger.info("Created " + entityType + " " + entityName);
      } else {
        logger.warning("Create failed, trying update");
        String updateCommand = String.format(UPDATE_COMMAND_MAP.get(type), entityName, entityPath);
        logger.fine(updateCommand);
        if (shell.executeScriptLine(updateCommand)) {
          logger.info("Updated " + entityType + " " + entityName);
        } else {
          logger.severe("Couldn't create or update " + entityType + " " + entityName);
          failedFor.add(entityName);
        }
      }
    }
  }
}
