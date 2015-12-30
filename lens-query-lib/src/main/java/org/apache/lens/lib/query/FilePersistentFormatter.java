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
package org.apache.lens.lib.query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.query.PersistedOutputFormatter;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import lombok.extern.slf4j.Slf4j;

/**
 * File formatter for {@link PersistedOutputFormatter}
 * <p></p>
 * This is a {@link WrappedFileFormatter} which can wrap any {@link FileFormatter}.
 */
@Slf4j
public class FilePersistentFormatter extends WrappedFileFormatter implements PersistedOutputFormatter {

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.lib.query.WrappedFileFormatter#init(org.apache.lens.server.api.query.QueryContext,
   * org.apache.lens.server.api.driver.LensResultSetMetadata)
   */
  public void init(QueryContext ctx, LensResultSetMetadata metadata) throws IOException {
    super.init(ctx, metadata);
  }

  // File names are of the form 000000_0

  /**
   * The Class PartFile.
   */
  class PartFile implements Comparable<PartFile> {

    /**
     * The id.
     */
    int id;

    /**
     * Instantiates a new part file.
     *
     * @param fileName the file name
     * @throws ParseException the parse exception
     */
    PartFile(String fileName) throws ParseException {
      String idStr = fileName.substring(0, fileName.lastIndexOf('_'));
      id = Integer.parseInt(idStr);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(PartFile other) {
      if (this.id < other.id) {
        return -1;
      } else if (this.id > other.id) {
        return 1;
      }
      return 0;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.query.PersistedOutputFormatter#addRowsFromPersistedPath(org.apache.hadoop.fs.Path)
   */
  @Override
  public void addRowsFromPersistedPath(Path persistedDir) throws IOException {
    FileSystem persistFs = persistedDir.getFileSystem(ctx.getConf());

    FileStatus[] partFiles = persistFs.listStatus(persistedDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        if (path.getName().startsWith("_")) {
          return false;
        }
        return true;
      }
    });

    TreeMap<PartFile, FileStatus> partFileMap = new TreeMap<PartFile, FileStatus>();
    try {
      for (FileStatus file : partFiles) {
        partFileMap.put(new PartFile(file.getPath().getName()), file);
      }

      for (Map.Entry<PartFile, FileStatus> entry : partFileMap.entrySet()) {
        log.info("Processing file:{}", entry.getValue().getPath());
        BufferedReader in = null;
        try {
          // default encoding in hadoop filesystem is utf-8
          in = new BufferedReader(new InputStreamReader(persistFs.open(entry.getValue().getPath()), "UTF-8"));
          String row = in.readLine();
          while (row != null) {
            writeRow(row);
            row = in.readLine();
          }
        } finally {
          if (in != null) {
            in.close();
          }
        }
      }
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }
}
