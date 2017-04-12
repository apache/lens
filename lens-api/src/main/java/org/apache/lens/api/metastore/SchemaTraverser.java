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
import java.io.FilenameFilter;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.collect.Maps;

/*
 * Created on 07/03/17.
 */
public class SchemaTraverser implements Runnable {
  final File parent;
  final Map<String, Class<?>> types = Maps.newLinkedHashMap();
  private final SchemaEntityProcessor action;
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
  private static final FilenameFilter XML_FILTER = (dir, name) -> name.endsWith(".xml");

  public interface SchemaEntityProcessor extends BiConsumer<File, Class<?>> {
  }

  public SchemaTraverser(File parent, SchemaEntityProcessor action) {
    this.parent = parent;
    this.action = action;
  }

  @Override
  public void run() {
    for (Map.Entry<String, Class<?>> entry : types.entrySet()) {
      File f = new File(parent, entry.getKey());
      if (f.exists()) {
        assert f.isDirectory();
        File[] files = f.listFiles(XML_FILTER);
        if (files != null) {
          for (File entityFile : files) {
            action.accept(entityFile.getAbsoluteFile(), entry.getValue());
          }
        }
      }
    }
  }
}
