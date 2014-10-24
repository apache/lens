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
 * The Class LensDimensionCommands.
 */
@Component
public class LensDimensionCommands extends BaseLensCommand implements CommandMarker {

  /**
   * Show dimensions.
   *
   * @return the string
   */
  @CliCommand(value = "show dimensions", help = "show list of dimensions in database")
  public String showDimensions() {
    List<String> dimensions = getClient().getAllDimensions();
    if (dimensions != null) {
      return Joiner.on("\n").join(dimensions);
    } else {
      return "No Dimensions found";
    }
  }

  /**
   * Creates the dimension.
   *
   * @param dimensionSpec
   *          the dimension spec
   * @return the string
   */
  @CliCommand(value = "create dimension", help = "Create a new Dimension")
  public String createDimension(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<path to dimension-spec file>") String dimensionSpec) {
    File f = new File(dimensionSpec);

    if (!f.exists()) {
      return "dimension spec path" + f.getAbsolutePath() + " does not exist. Please check the path";
    }
    APIResult result = getClient().createDimension(dimensionSpec);

    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "create dimension succeeded";
    } else {
      return "create dimension failed";
    }
  }

  /**
   * Drop dimension.
   *
   * @param dimension
   *          the dimension
   * @return the string
   */
  @CliCommand(value = "drop dimension", help = "drop dimension")
  public String dropDimension(
      @CliOption(key = { "", "table" }, mandatory = true, help = "dimension name to be dropped") String dimension) {
    APIResult result = getClient().dropDimension(dimension);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully dropped " + dimension + "!!!";
    } else {
      return "Dropping dimension failed";
    }
  }

  /**
   * Update dimension.
   *
   * @param specPair
   *          the spec pair
   * @return the string
   */
  @CliCommand(value = "update dimension", help = "update dimension")
  public String updateDimension(
      @CliOption(key = { "", "dimension" }, mandatory = true, help = "<dimension-name> <path to dimension-spec file>") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " + "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[1]);

    if (!f.exists()) {
      return "Fact spec path" + f.getAbsolutePath() + " does not exist. Please check the path";
    }

    APIResult result = getClient().updateDimension(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Update of " + pair[0] + " succeeded";
    } else {
      return "Update of " + pair[0] + " failed";
    }
  }

  /**
   * Describe dimension.
   *
   * @param dimensionName
   *          the dimension name
   * @return the string
   */
  @CliCommand(value = "describe dimension", help = "describe dimension")
  public String describeDimension(
      @CliOption(key = { "", "dimension" }, mandatory = true, help = "<dimension-name>") String dimensionName) {
    try {
      return formatJson(mapper.writer(pp).writeValueAsString(getClient().getDimension(dimensionName)));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
