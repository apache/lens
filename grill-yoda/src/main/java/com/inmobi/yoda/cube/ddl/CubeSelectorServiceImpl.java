package com.inmobi.yoda.cube.ddl;

import com.inmobi.grill.api.GrillException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;

public class CubeSelectorServiceImpl implements CubeSelectorService {
  public static final Logger LOG = LogManager.getLogger(CubeSelectorServiceImpl.class);
  private List<Table> allTables;
  private Set<String> allDimensionColumns;

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
      // Set SessionState if not set in current thread.
      HiveConf hconf = new HiveConf(conf, CubeSelectorService.class);
      if (null == SessionState.get()) {
        LOG.debug("@@ Creating new session state for selector service");
        SessionState.start(hconf);
      }
      allTables = new ArrayList<Table>();
      allDimensionColumns = new HashSet<String>();
      Set<String> allMeasureColumns = new HashSet<String>();

      CubeMetastoreClient metastore = CubeMetastoreClient.getInstance(hconf);
      LOG.debug("@@ Current DB: " + metastore.getCurrentDatabase());

      for (Cube cube : metastore.getAllCubes()) {
        LOG.debug("@@ Processing cube " + cube.getName());
        Table tab = new Table();
        tab.table = cube;
        String costKey = "cube." + cube.getName().substring("cube_".length()) + ".cost";
        String costStr = cube.getProperties().get(costKey);
        tab.cost = costStr == null ? Integer.MAX_VALUE : Integer.valueOf(costStr);

        String pathKey = "cube." + cube.getName().substring("cube_".length()) + ".path";
        String pathStr = cube.getProperties().get(pathKey);
        tab.path = pathStr;
        LOG.debug("@@ " + cube.getName() + " path: [" + pathStr + "]");

        tab.columns = new HashSet<String>();

        for (CubeMeasure m : cube.getMeasures()) {
          tab.columns.add(m.getName());
          allMeasureColumns.add(m.getName());
        }

        for (CubeDimension d : cube.getDimensions()) {
          tab.columns.add(d.getName());
          allDimensionColumns.add(d.getName());
        }
        allTables.add(tab);
      }

      for (CubeDimensionTable dim : metastore.getAllDimensionTables()) {
        LOG.debug("@@ Processing dimension " + dim.getName());
        Table tab = new Table();
        tab.table = dim;
        tab.columns = new HashSet<String>();

        for (FieldSchema f : dim.getColumns()) {
          tab.columns.add(f.getName());
          allDimensionColumns.add(f.getName());
        }
        allTables.add(tab);
        LOG.debug("Total tables loaded " + allTables.size());
      }

      // Check for conflicts between dimension and measure names
      if (allMeasureColumns.retainAll(allDimensionColumns)) {
        if (!allMeasureColumns.isEmpty())
        LOG.warn("Columns declared as both measures and dimensions: " + allMeasureColumns.toString());
      }
    } catch (HiveException e) {
      LOG.error("Error caching cubes metastore table", e);
      throw new GrillException(e);
    }
  }

  /**
   * Select only the cubes which have all the dimensions and at least one measure.
   * 
   * Assume we can check if a column is dimension based on its name. Assumption is that a column with same name will not be
   * dim in one table and measure in another.
   * If a cube can answer all columns, return that cube
   * If multiple cubes can answer all columns, return min cost cube from each path group.
   * 
   * @param columns
   * 
   * @return Map of set of columns to set of cube tables
   */
  @Override
  public Map<Set<String>, Set<AbstractCubeTable>> select(Collection<String> columns) {
    Map<Table, Set<String>> selection = new HashMap<Table, Set<String>>();
    Set<String> dimensions = new HashSet<String>();
    List<String> nonDimensions = new ArrayList<String>();

    for (String c : columns) {
      if (allDimensionColumns.contains(c)) {
        dimensions.add(c);
      } else {
        nonDimensions.add(c);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("@@ Dims: " + dimensions.toString());
      LOG.debug("@@ Non Dims: " + nonDimensions.toString());
    }

    // find all matching cubes
    final Map<Table, Integer> measureCount = new HashMap<Table, Integer>();
    for (Table table : allTables) {
      // Only choose cubes that have all the dimensions in the column arguments
      if (table.columns.containsAll(dimensions)) {
        if (nonDimensions.isEmpty()) {
          // We have to select only for dimension columns, so add this table to the selection
          Set<String> subset = selection.get(table);
          if (subset == null) {
            subset = new LinkedHashSet<String>(dimensions);
            selection.put(table, subset);
          }
        } else {
          for (String nonDimColumn : nonDimensions) {
            // Table to be selected should have at least one non-dim column
            if (table.columns.contains(nonDimColumn)) {
              Set<String> subset = selection.get(table);
              if (subset == null) {
                subset = new LinkedHashSet<String>(dimensions);
                selection.put(table, subset);
              }
              subset.add(nonDimColumn);
              measureCount.put(table, measureCount.get(table) == null ? 1 : measureCount.get(table) + 1);
            }
          }
        }
      }
    }

    // Group cubes by paths
    if (!nonDimensions.isEmpty()) {
      Map<String, List<Table>> cubeGroup = new HashMap<String,  List<Table>>();
      for (Map.Entry<Table, Set<String>> entry : selection.entrySet()) {
        Table table = entry.getKey();
        String groupKey = table.path;
        List<Table> group = cubeGroup.get(groupKey);
        if (group == null) {
          group = new ArrayList<Table>();
          cubeGroup.put(groupKey, group);
        }
        group.add(table);
      }

      // For each path group, retain only the cube which has max measures
      for (String path : cubeGroup.keySet()) {
        List<Table> group = cubeGroup.get(path);
        Table maxMsrCube = Collections.max(group, new Comparator<Table>() {
          @Override
          public int compare(Table table, Table table2) {
            return measureCount.get(table) - measureCount.get(table2);
          }
        });

        group.remove(maxMsrCube);
        selection.keySet().removeAll(group);
      }
    }

    // Check if there are cubes that contains all the columns
    List<Table> tabsWithAllColumns = new ArrayList<Table>();
    for (Table tab : selection.keySet()) {
      if (selection.get(tab).containsAll(columns)) {
        tabsWithAllColumns.add(tab);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("@@ Cube with all columns matching " + tabsWithAllColumns);
    }
    if (!tabsWithAllColumns.isEmpty()) {
      // Return only the cubes that have all the columns
      selection.keySet().retainAll(tabsWithAllColumns);
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
