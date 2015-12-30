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
package org.apache.lens.api.util;

import java.io.File;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.api.LensConf;

/**
 * The Path validator utility class takes in the malformatted paths
 * and converts to the appropriate valid path.
 * It also takes in custom properties for special handling of URI's.
 */
public class PathValidator {
  private LensConf config;

  public static final String PATH_PREFIX = "filesystem.prefix.";

  public PathValidator(LensConf config) {
    createDefaultUriProperties();
    this.config.addProperties(config.getProperties());
  }

  private void createDefaultUriProperties() {
    config = new LensConf();
    config.addProperty(PATH_PREFIX + "hdfs", "://");
    config.addProperty(PATH_PREFIX + "s3", "://");
    config.addProperty(PATH_PREFIX + "s3n", "://");
  }

  /**
   * Converts the input path to appropriate File/URI path.
   * Also removes erroneously appended prefix for URI's.
   * Takes additional properties for special URI handling.
   *
   * @param path input path
   * @param shouldBeDirectory should be a directory
   * @param shouldExist should exist
   * @return converted path
   */
  public String getValidPath(File path, boolean shouldBeDirectory,
                                       boolean shouldExist) {
    /** For URI located file paths **/
    /* Cli prefixes the local path before file:/ if provided path does not start with ~/ or / */
    if (path.getPath().indexOf(":/") > 0) {
      return removePrefixBeforeURI(path.getPath());
    }

    /** For filesystem file paths **/
    if (shouldExist && !path.exists()) {
      throw new RuntimeException("Path " + path + " doesn't exist.");
    }
    if (shouldBeDirectory && !path.isDirectory()) {
      throw new RuntimeException("Path " + path + " is not a directory");
    }
    if (!shouldBeDirectory && path.isDirectory()) {
      throw new RuntimeException("Path " + path + " is a directory");
    }
    return path.getAbsolutePath();
  }

  /**
   * The CLI erroneously appends absolute path for URI's.
   * It also replaces the double slashes to single slashes.
   * Eg. /home/some/path/file:/home/git/lens/target/tempdata_*.txt
   *
   * This Util method removes the erroneously appended absolute path for URI's.
   * And appends :/// for the URI's.
   * Paths with hdfs/s3/s3n are appended with double slashes ://
   *
   * Any new URI's have to be handled appropriately
   *
   * @param path input path
   * @return cleaned up path
   */
  public String removePrefixBeforeURI(String path) {
    /**
     * For uniformity, Replaces all multiple slashes to single slash.
     * This is how the CLI forwards the path.
     * Any URI scheme with multiple slashes has to be handled appropriately.
     **/
    path = path.replaceAll("/+", "/");
    path = path.replaceAll("/$", "");
    if (path.startsWith("~")) {
      path = path.replaceFirst("~", System.getProperty("user.home"));
    }

    /** For URI located file paths **/
    /* Cli prefixes the local path before URI if provided path does not start with ~/ or / */
    String projectDir = Paths.get("").toAbsolutePath().toString();
    int indexOfUriInit = path.indexOf(":/");
    int currDirLen = projectDir.length();
    String escapedSlashes = ":///";

    /**
     * Special case - hdfs/s3/s3n need double escape :// instead of :///
     * Currently add handling only for hdfs/s3/s3n uri's. Open to identify corner cases.
     * Can be extended to handle other uri patterns.
     */
    Pattern pattern = Pattern.compile("^(.*):/.*");
    Matcher match = pattern.matcher(path);
    if (match.find() && config.getProperties().get(PATH_PREFIX + match.group(1)) != null) {
      escapedSlashes = config.getProperties().get(PATH_PREFIX + match.group(1));
    }

    /** Remove the prefix **/
    if (path.startsWith(projectDir) && indexOfUriInit != -1) {
      path = path.substring(currDirLen + 1, path.length());
    }

    /** Properly format the URI **/
    indexOfUriInit = path.indexOf(":/");
    if (indexOfUriInit != -1 && !path.contains(":///")) {
      path = path.substring(0, indexOfUriInit)
          + escapedSlashes
          + path.substring(indexOfUriInit + 2);
    }
    return path;
  }
}
