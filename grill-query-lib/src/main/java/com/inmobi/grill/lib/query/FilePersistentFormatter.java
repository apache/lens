package com.inmobi.grill.lib.query;

/*
 * #%L
 * Grill Query Library
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;

import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.query.PersistedOutputFormatter;

public class FilePersistentFormatter extends FileFormatter implements PersistedOutputFormatter {

  private int numRows = 0;

  @Override
  public void writeHeader() throws IOException {
    String header = ctx.getConf().get(GrillConfConstants.QUERY_OUTPUT_HEADER);
    if (!StringUtils.isBlank(header)) {
      rowWriter.write(null, new Text(header));
    }
  }

  @Override
  public void writeFooter() throws IOException {
    String footer = ctx.getConf().get(GrillConfConstants.QUERY_OUTPUT_FOOTER);
    if (!StringUtils.isBlank(footer)) {
      rowWriter.write(null, new Text(footer));
    }

  }

  // File names are of the form 000000_0
  class PartFile implements Comparable<PartFile> {
    int id;

    PartFile(String fileName) throws ParseException {
      String idStr = fileName.substring(0, fileName.lastIndexOf('_'));
      id = Integer.parseInt(idStr);
    }

    @Override
    public int compareTo(PartFile other) {
      if (this.id < other.id) {
        return -1;
      } else if  (this.id > other.id) {
        return 1;
      }
      return 0;
    }


  }
  @Override
  public void addRowsFromPersistedPath(Path persistedDir) throws IOException {
    FileSystem persistFs =  persistedDir.getFileSystem(getConf());

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
        BufferedReader in = new BufferedReader(new InputStreamReader(
            persistFs.open(entry.getValue().getPath())));
        String row = in.readLine();
        while (row != null) {
          writeRow(row);
          numRows++;
          row = in.readLine();
        }
      }
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int getNumRows() {
    return numRows;
  }

}
