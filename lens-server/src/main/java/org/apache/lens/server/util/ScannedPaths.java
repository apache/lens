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

package org.apache.lens.server.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScannedPaths implements Iterable<String> {
  private String path = null;
  private String type = null;

  private Path fsPath;

  /** Is the provided expression parsed **/
  private boolean isScanned;

  /* Keep all matched paths so we don't have to query filesystem multiple times */
  private Map<String, String> matchedPaths = null;

  /* The Chosen Ones */
  @Getter(lazy=true) private final List<String> finalPaths = getMatchedPaths();

  public ScannedPaths(String path, String type) {
    this.path = path;
    this.type = type;
  }

  @Override
  public Iterator<String> iterator() {
    /** Does all the pattern matching and returns the iterator to finalPaths collection **/
    return (getFinalPaths() == null) ? null : getFinalPaths().iterator();
  }

  /**
   * Method that computes path of resources matching the input path or path regex pattern.
   * If provided path is a directory it additionally checks for the jar_order or glob_order file
   * that imposes ordering of resources and filters out other resources.
   *
   * Updates finalPaths List with matched paths and returns an iterator for matched paths.
   */
  private List<String> getMatchedPaths() {
    List<String> finalPaths = null;
    try {
      FileSystem fs = FileSystem.get(new URI(path), new Configuration());
      fsPath = new Path(new URI(path).getPath());

      if (fs.isDirectory(fsPath)) {
        findAllMatchedPaths(true);
        filterByJarType();
      /* Updates finalPaths List with restrictions imposed
         by jar_order/glob_order file */
        finalPaths = getMatchedPathsFilteredByOrder();
      } else {
        findAllMatchedPaths(false);
        filterByJarType();
        if (matchedPaths != null) {
          finalPaths = new ArrayList<String>(matchedPaths.values());
        }
      }
    } catch (FileNotFoundException fex) {
      log.error("File not found while scanning path.", fex);
    } catch (URISyntaxException | IOException ex) {
      log.error("Exception while initializing PathScanner.", ex);
    }
    return finalPaths;
  }

  /**
   * Populates the matchedPaths[] with all paths matching the pattern.
   */
  private void findAllMatchedPaths(boolean isDir) {
    try {
      Path path = isDir ? new Path(fsPath, "*") : fsPath;
      FileStatus[] statuses = path.getFileSystem(new Configuration()).globStatus(path);

      if (statuses == null || statuses.length == 0) {
        log.info("No matched paths found for expression " + path);
        return;
      }
      matchedPaths = new HashMap<String, String>();
      for (int count = 0; count < statuses.length; count++) {
        matchedPaths.put(statuses[count].getPath().getName(), statuses[count].getPath().toString());
      }
    } catch (FileNotFoundException fex) {
      log.error("File not found while scanning path.", fex);
      return;
    } catch (IOException ioex) {
      log.error("IOException while scanning path.", ioex);
      return;
    }
  }

  /**
   * Filters the matchedPaths by "jar" type.
   * Removes non-jar resources if type is specified as "jar".
   */
  private void filterByJarType() {
    if (matchedPaths == null) {
      return;
    } else if (type.equalsIgnoreCase("jar")) {
      Iterator<Map.Entry<String, String>> iter = matchedPaths.entrySet().iterator();

      while (iter.hasNext()) {
        Map.Entry<String, String> entry = iter.next();
        if (!entry.getKey().endsWith(".jar")) {
          iter.remove();
        }
      }
    }
  }

  /**
   * Filters the matchedPath[] to remove unwanted resources
   * and apply ordering to the resources as specified in jar_order or glob_order file.
   * Bypasses filtering if none of the files is present in the directory.
   */
  private List<String> getMatchedPathsFilteredByOrder() {
    if (matchedPaths == null) {
      return  null;
    }

    List<String> finalPaths = null;
    InputStream resourceOrderIStream = null;
    List<String> resources;
    try {
      FileSystem fs = fsPath.getFileSystem(new Configuration());
      Path resourceOrderFile = new Path(fsPath.toUri().getPath(), "jar_order");

      if (!fs.exists(resourceOrderFile)) {
        resourceOrderFile = new Path(fsPath.toUri().getPath(), "glob_order");
        if (!fs.exists(resourceOrderFile)) {
          /* No order file present. Bypass filtering and Add all resource matching pattern */
          return new ArrayList<>(matchedPaths.values());
        }
      }

      resourceOrderIStream = fs.open(resourceOrderFile);
      resources = IOUtils.readLines(resourceOrderIStream, Charset.forName("UTF-8"));
      finalPaths = new ArrayList<>();
      for(String resource : resources) {
        if (resource == null || resource.isEmpty()) {
          continue;
        }

        if (matchedPaths.containsKey(resource)) {
          finalPaths.add(matchedPaths.get(resource));
        }
      }
    } catch (FileNotFoundException fex) {
      log.error("File not found while scanning path.", fex);
      finalPaths = null;
    } catch (IOException ioex) {
      log.error("IOException while scanning path.", ioex);
      finalPaths = null;
    } finally {
      IOUtils.closeQuietly(resourceOrderIStream);
    }
    return finalPaths;
  }
}
