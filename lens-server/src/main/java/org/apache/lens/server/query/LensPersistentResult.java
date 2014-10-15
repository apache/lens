package org.apache.lens.server.query;

/*
 * #%L
 * Lens Server
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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

import org.apache.lens.api.LensException;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.PersistentResultSet;


public class LensPersistentResult extends PersistentResultSet {

  private final LensResultSetMetadata metadata;
  private final String outputPath;
  private final int numRows;

  public LensPersistentResult(LensResultSetMetadata metadata,
      String outputPath, int numRows) {
    this.metadata = metadata;
    this.outputPath = outputPath;
    this.numRows = numRows;
  }

  @Override
  public String getOutputPath() throws LensException {
    return outputPath;
  }

  @Override
  public int size() throws LensException {
    return numRows;
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    return metadata;
  }
}
