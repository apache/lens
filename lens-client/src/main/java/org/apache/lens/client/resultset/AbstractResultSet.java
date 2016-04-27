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

package org.apache.lens.client.resultset;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.lens.client.exceptions.LensClientIOException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractResultSet implements ResultSet {

  @Getter
  private InputStream inStream;
  @Getter
  private Charset encoding;
  @Getter
  private char delimiter;
  @Getter
  private boolean isHeaderRowPresent;

  protected ResultSetReader reader;
  protected int totalRowsRead;
  protected String[] columnNames;

  public AbstractResultSet(InputStream inStream, Charset encoding, boolean isHeaderRowPresent, char delimiter)
    throws LensClientIOException {
    this.inStream = inStream;
    this.encoding = encoding;
    this.isHeaderRowPresent = isHeaderRowPresent;
    this.delimiter = delimiter;
    init();
  }

  protected void init() throws LensClientIOException {
    reader = createResultSetReader();
    if (isHeaderRowPresent && reader.next()) {
      columnNames = reader.getRow();
      log.info("Resultset column names : " + Arrays.asList(columnNames));
    }
  }

  /**
   * Creates a reader which be used for reading rows of resultset.
   */
  protected abstract ResultSetReader createResultSetReader() throws LensClientIOException;

  @Override
  public String[] getColumnNames() throws LensClientIOException {
    return columnNames;
  }

  @Override
  public boolean next() throws LensClientIOException {
    boolean hasNext = reader.next();
    if (hasNext) {
      totalRowsRead++;
    } else {
      close();
    }
    return hasNext;
  }

  @Override
  public String[] getRow() throws LensClientIOException {
    return reader.getRow();
  }

  /**
   * Called after the last row form the result set is read.
   */
  protected void close() throws LensClientIOException {
    log.info("Total rows from resultset : " + totalRowsRead);
    try {
      inStream.close();
    } catch (IOException e) {
      log.error("Error while closing result stream", e);
    }
  }
}
