package com.inmobi.yoda.cube.ddl;


import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface CubeSelectorService {
  /**
   * Return all cubes which contain a non empty subset of the columns specified as part of the argument
   *
   * If multiple cubes contain the same subset of columns, and their paths are same then the cube with least cost is
   * returned.
   *
   * @param columns
   * @return Map of Cube to the subset of columns contained in that cube
   */
  public Map<List<String>, List<AbstractCubeTable>> select(Collection<String> columns);
}
