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
