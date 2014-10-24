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
 * The Class LensCubeCommands.
 */
@Component
public class LensCubeCommands extends BaseLensCommand implements CommandMarker {

  /**
   * Show cubes.
   *
   * @return the string
   */
  @CliCommand(value = "show cubes", help = "show list of cubes in database")
  public String showCubes() {
    List<String> cubes = getClient().getAllCubes();
    if (cubes != null) {
      return Joiner.on("\n").join(cubes);
    } else {
      return "No Cubes found";
    }
  }

  /**
   * Creates the cube.
   *
   * @param cubeSpec
   *          the cube spec
   * @return the string
   */
  @CliCommand(value = "create cube", help = "Create a new Cube")
  public String createCube(
      @CliOption(key = { "", "table" }, mandatory = true, help = "<path to cube-spec file>") String cubeSpec) {
    File f = new File(cubeSpec);

    if (!f.exists()) {
      return "cube spec path" + f.getAbsolutePath() + " does not exist. Please check the path";
    }
    APIResult result = getClient().createCube(cubeSpec);

    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "create cube succeeded";
    } else {
      return "create cube failed";
    }
  }

  /**
   * Drop cube.
   *
   * @param cube
   *          the cube
   * @return the string
   */
  @CliCommand(value = "drop cube", help = "drop cube")
  public String dropCube(
      @CliOption(key = { "", "table" }, mandatory = true, help = "cube name to be dropped") String cube) {
    APIResult result = getClient().dropCube(cube);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully dropped " + cube + "!!!";
    } else {
      return "Dropping cube failed";
    }
  }

  /**
   * Update cube.
   *
   * @param specPair
   *          the spec pair
   * @return the string
   */
  @CliCommand(value = "update cube", help = "update cube")
  public String updateCube(
      @CliOption(key = { "", "cube" }, mandatory = true, help = "<cube-name> <path to cube-spec file>") String specPair) {
    Iterable<String> parts = Splitter.on(' ').trimResults().omitEmptyStrings().split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " + "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[1]);

    if (!f.exists()) {
      return "Fact spec path" + f.getAbsolutePath() + " does not exist. Please check the path";
    }

    APIResult result = getClient().updateCube(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Update of " + pair[0] + " succeeded";
    } else {
      return "Update of " + pair[0] + " failed";
    }
  }

  /**
   * Describe cube.
   *
   * @param cubeName
   *          the cube name
   * @return the string
   */
  @CliCommand(value = "describe cube", help = "describe cube")
  public String describeCube(@CliOption(key = { "", "cube" }, mandatory = true, help = "<cube-name>") String cubeName) {
    try {
      return formatJson(mapper.writer(pp).writeValueAsString(getClient().getCube(cubeName)));

    } catch (IOException e) {
      throw new IllegalArgumentException(e);

    }
  }
}
