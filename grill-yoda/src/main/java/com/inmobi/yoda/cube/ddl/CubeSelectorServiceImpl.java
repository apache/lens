package com.inmobi.yoda.cube.ddl;

import com.inmobi.grill.exception.GrillException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.*;

public class CubeSelectorServiceImpl implements CubeSelectorService {
  private List<Table> allTables;

  private class Table {
    String path;
    int cost;
    AbstractCubeTable table;
    Set<String> columns;

    @Override
    public int hashCode() {
      return table.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Table) {
        return table.equals(((Table)o).table);
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      return table.getName() + "|" + path + "|" + cost;
    }
  }

  public CubeSelectorServiceImpl(Configuration conf) throws GrillException {
    try {
      allTables = new ArrayList<Table>();
      CubeMetastoreClient metastore = CubeMetastoreClient.getInstance(new HiveConf(conf, CubeSelectorService.class));

      for (Cube cube : metastore.getAllCubes()) {
        Table tab = new Table();
        tab.table = cube;
        String costKey = "cube." + cube.getName().substring("cube_".length()) + ".cost";
        String costStr = cube.getProperties().get(costKey);
        tab.cost = costStr == null ? Integer.MAX_VALUE : Integer.valueOf(costStr);

        String pathKey = "cube." + cube.getName().substring("cube_".length()) + ".path";
        String pathStr = cube.getProperties().get(pathKey);
        tab.path = pathStr;

        tab.columns = new HashSet<String>();

        for (CubeMeasure m : cube.getMeasures()) {
          tab.columns.add(m.getName());
        }

        for (CubeDimension d : cube.getDimensions()) {
          tab.columns.add(d.getName());
        }
        allTables.add(tab);
      }

      for (CubeDimensionTable dim : metastore.getAllDimensionTables()) {
        Table tab = new Table();
        tab.table = dim;
        tab.columns = new HashSet<String>();

        for (FieldSchema f : dim.getColumns()) {
          tab.columns.add(f.getName());
        }
        allTables.add(tab);
      }

    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }


  @Override
  public Map<Set<String>, Set<AbstractCubeTable>> select(Collection<String> columns) {
    Map<Table, Set<String>> selection = new HashMap<Table, Set<String>>();

    // find all matching cubes
    for (Table table : allTables) {
      for (String column : columns) {
        if (table.columns.contains(column)) {
          Set<String> subset = selection.get(table);
          if (subset == null) {
            subset = new LinkedHashSet<String>();
            selection.put(table, subset);
          }
          subset.add(column);
        }
      }
    }

    // Group cubes by column subset and paths
    Map<String, List<Table>> cubeGroup = new HashMap<String,  List<Table>>();
    for (Map.Entry<Table, Set<String>> entry : selection.entrySet()) {
      Table table = entry.getKey();
      Set<String> subset = entry.getValue();

      StringBuilder buf = new StringBuilder();
      if (table.table instanceof Cube) {
        buf.append("C");
      } else if (table.table instanceof CubeDimensionTable) {
        buf.append("D");
      }
      for (String column : subset) {
        buf.append(column);
      }

      String groupKey = buf.append(table.path == null? "" : table.path).toString();
      List<Table> group = cubeGroup.get(groupKey);
      if (group == null) {
        group = new ArrayList<Table>();
        cubeGroup.put(groupKey, group);
      }
      group.add(table);
    }

    // In each group retain only the min cost cube
    for (List<Table> group : cubeGroup.values()) {
      Table minTable = group.get(0);
      for (int i = 1; i < group.size(); i++) {
        Table table = group.get(i);
        if (table.cost < minTable.cost) {
          minTable = table;
        }
      }

      // Remove cubes with higher costs and same paths from selection
      for (Table table : group) {
        if (minTable != table) {
          selection.remove(table);
        }
      }
    }

    // Invert the map
    Map<Set<String>, Set<AbstractCubeTable>> result = new HashMap<Set<String>, Set<AbstractCubeTable>>();
    for (Map.Entry<Table, Set<String>> entry : selection.entrySet()) {
      Set<String> cols = entry.getValue();
      Set<AbstractCubeTable> tabs = result.get(cols);
      if (tabs == null) {
        tabs = new HashSet<AbstractCubeTable>();
        result.put(cols, tabs);
      }
      tabs.add(entry.getKey().table);
    }
    return result;
  }
}
