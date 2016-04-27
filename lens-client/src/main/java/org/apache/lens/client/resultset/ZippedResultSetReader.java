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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.lens.client.exceptions.LensClientIOException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ZippedResultSetReader implements ResultSetReader {

  @Getter
  private ZipInputStream zipStream;
  @Getter
  private Charset encoding;
  @Getter
  private char delimiter;
  @Getter
  private boolean isHeaderRowPresent;

  private ResultSetReader actualReader;

  public ZippedResultSetReader(InputStream inStream, Charset encoding, char delimiter, boolean isHeaderRowPresent)
    throws LensClientIOException {

    this.zipStream = new ZipInputStream(inStream);
    this.encoding = encoding;
    this.delimiter = delimiter;
    this.isHeaderRowPresent = isHeaderRowPresent;

    getNextEntry(); // Move the cursor to the first entyry in the zip
    this.actualReader = createEntryReader();
  }

  @Override
  public String[] getRow() throws LensClientIOException {
    return actualReader.getRow();
  }

  @Override
  public boolean next() throws LensClientIOException {
    if (actualReader.next()) {
      return true;
    } else if (getNextEntry() != null) {
      actualReader = createEntryReader(); //created reader for getRow entry in zip file
      if (isHeaderRowPresent) {
        actualReader.next(); //skip the header row in all but first entry
      }
      return next();
    }
    return false;
  }

  private ZipEntry getNextEntry() throws LensClientIOException {
    try {
      ZipEntry entry = zipStream.getNextEntry();
      if (entry != null) {
        log.info("Reading entry :" + entry.getName());
      }
      return entry;
    } catch (IOException e) {
      log.error("Unable to read zip entry", e);
      throw new LensClientIOException("Unable to read zip entry", e);
    }
  }

  protected abstract ResultSetReader createEntryReader() throws LensClientIOException;
}
