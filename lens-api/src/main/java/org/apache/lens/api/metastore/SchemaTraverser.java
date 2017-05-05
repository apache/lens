/*
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
package org.apache.lens.api.metastore;

import java.io.File;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.collect.Maps;
import lombok.Data;

/*
 * Created on 07/03/17.
 */
@Data
public class SchemaTraverser implements Runnable {
  private final File parent;
  private final SchemaEntityProcessor action;
  private final String entityTypeFilter;
  private final String fileNameFilter;
  final Map<String, Class<?>> types = Maps.newLinkedHashMap();

  {
    types.put("storages", XStorage.class);
    types.put("cubes/base", XBaseCube.class);
    types.put("cubes/derived", XDerivedCube.class);
    types.put("dimensions", XDimension.class);
    types.put("facts", XFactTable.class);
    types.put("dimtables", XDimensionTable.class);
    types.put("dimensiontables", XDimensionTable.class);
    types.put("dimensiontables", XDimensionTable.class);
    types.put("segmentations", XSegmentation.class);
  }

  public interface SchemaEntityProcessor extends BiConsumer<File, Class<?>> {
  }

  @Override
  public void run() {
    for (Map.Entry<String, Class<?>> entry : types.entrySet()) {
      if (entityTypeFilter == null || entry.getKey().contains(entityTypeFilter)) {
        File f = new File(parent, entry.getKey());
        if (f.exists()) {
          assert f.isDirectory();
          File[] files = f.listFiles((dir, name) -> name.endsWith(".xml")
            && (fileNameFilter == null || name.toLowerCase().contains(fileNameFilter.toLowerCase())));
          if (files != null) {
            for (File entityFile : files) {
              action.accept(entityFile.getAbsoluteFile(), entry.getValue());
            }
          }
        }
      }
    }
  }
}
