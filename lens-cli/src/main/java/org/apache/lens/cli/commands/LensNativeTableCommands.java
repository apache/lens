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
import org.apache.lens.api.metastore.XNativeTable;
import org.apache.lens.cli.commands.annotations.UserDocumentation;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * The Class LensNativeTableCommands.
 */
@Component
@UserDocumentation(title = "Commands for Native Table Management", description = "Read operations on native tables")
public class LensNativeTableCommands extends LensCRUDCommand<XNativeTable> {

  /**
   * Show native tables.
   *
   * @return the string
   */
  @CliCommand(value = "show nativetables", help = "show list of native tables belonging to current database")
  public String showNativeTables() {
    return showAll();
  }

  /**
   * Describe native table.
   *
   * @param name the tbl name
   * @return the string
   */
  @CliCommand(value = "describe nativetable", help = "describe nativetable named <native-table-name>")
  public String describeNativeTable(
    @CliOption(key = {"", "name"}, mandatory = true, help = "<native-table-name>") String name) {
    return describe(name);
  }

  @Override
  public List<String> getAll() {
    return getClient().getAllNativeTables();
  }

  @Override
  protected APIResult doCreate(String path, boolean ignoreIfExists) {
    return null;
  }

  @Override
  protected XNativeTable doRead(String name) {
    return getClient().getNativeTable(name);
  }

  @Override
  public APIResult doUpdate(String name, String path) {
    return null;
  }

  @Override
  protected APIResult doDelete(String name, boolean cascade) {
    return null;
  }
}
