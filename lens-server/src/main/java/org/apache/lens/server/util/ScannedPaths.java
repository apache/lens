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
import java.util.Iterator;
import java.util.List;

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

  /* The Chosen Ones */
  @Getter(lazy=true) private final List<String> finalPaths = getMatchedPaths(path, type);

  public ScannedPaths(String path, String type) {
    this.path = path;
    this.type = type;
  }

  @Override
  public Iterator<String> iterator() {
    /** Does all the pattern matching and returns the iterator to finalPaths collection.
     *  finalPaths should never be null.
     **/
    return getFinalPaths().iterator();
  }

  /**
   * Method that computes path of resources matching the input path or path regex pattern.
   * If provided path is a directory it additionally checks for the jar_order or glob_order file
   * that imposes ordering of resources and filters out other resources.
   *
   * Updates finalPaths List with matched paths and returns an iterator for matched paths.
   */
  private List<String> getMatchedPaths(String path, String type) {
    List<String> finalPaths = new ArrayList<>();
    FileSystem fs;

    try {
      fs = FileSystem.get(new URI(path), new Configuration());
      Path pt = new Path(new URI(path));

      if (fs.exists(pt) && fs.isFile(pt)) {
        /**
         * CASE 1 : Direct FILE provided in path
         **/
        finalPaths.add(pt.toUri().toString());
      } else if (fs.exists(pt) && fs.isDirectory(pt)) {
        /**
         * CASE 2 : DIR provided in path
         **/
        Path resourceOrderFile = null;
        InputStream resourceOrderIStream = null;
        FileStatus[] statuses;
        List<String> newMatches;
        List<String> resources;
        boolean resourceFileFound = false;

        fs = pt.getFileSystem(new Configuration());
        resourceOrderFile = new Path(pt, "jar_order");
        /** Add everything in dir if no jar_order or glob_order is present **/
        if (!fs.exists(resourceOrderFile)) {
          resourceOrderFile = new Path(pt, "glob_order");
          if (!fs.exists(resourceOrderFile)) {
            /** Get matched resources recursively for all files **/
            statuses = fs.globStatus(new Path(pt, "*"));
            for (FileStatus st : statuses) {
              finalPaths.add(st.getPath().toUri().toString());
            }
          } else {
            resourceFileFound = true;
          }
        } else {
          resourceFileFound = true;
        }

        if (resourceFileFound) {
          /** Else get jars as per order specified in jar_order/glob_order **/
          resourceOrderIStream = fs.open(resourceOrderFile);
          resources = IOUtils.readLines(resourceOrderIStream, Charset.forName("UTF-8"));
          for (String resource : resources) {
            if (resource == null || resource.isEmpty()) {
              continue;
            }

            /** Get matched resources recursively for provided path/pattern **/
            if (resource.startsWith("/") || resource.contains(":/")) {
              newMatches = getMatchedPaths(new Path(resource).toString(), type);
            } else {
              newMatches = getMatchedPaths(new Path(pt, resource).toString(), type);
            }

            if (newMatches != null) {
              finalPaths.addAll(newMatches);
            }
          }
        }
      } else {
        /**
         * CASE 3 : REGEX provided in path
         * */
        FileStatus[] statuses = fs.globStatus(pt);
        for (FileStatus st : statuses) {
          List<String> newMatches = getMatchedPaths(st.getPath().toString(), type);
          if (newMatches != null) {
            finalPaths.addAll(newMatches);
          }
        }
      }

      filterDirsAndJarType(fs, finalPaths);

    } catch (FileNotFoundException fex) {
      log.error("File not found while scanning path.", fex);
    } catch (URISyntaxException | IOException ex) {
      log.error("Exception while initializing PathScanner.", ex);
    } catch (Exception e) {
      log.error("Exception while initializing PathScanner.", e);
    }

    return finalPaths;
  }


  /**
   * Skip Dirs from matched regex.
   * We are interested only in file resources.
   **/
  private void filterDirsAndJarType(FileSystem fs, List<String> matches) {
    try {
      Iterator<String> iter = matches.iterator();
      String path;
      while (iter.hasNext()) {
        path = iter.next();
        if (fs.isDirectory(new Path(path))) {
          iter.remove();
        } else if (type.equalsIgnoreCase("jar") && !path.endsWith(".jar")) {
          iter.remove();
        }
      }
    } catch (IOException e) {
      log.error("Exception while initializing filtering dirs.", e);
    }
  }
}
