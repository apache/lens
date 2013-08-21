package com.inmobi.yoda.cube.ddl;

import com.inmobi.grill.exception.GrillException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.*;

public class CubeSelectorServiceImpl implements CubeSelectorService {
  private List<Cube> allCubes;

  public CubeSelectorServiceImpl(Configuration conf) throws GrillException {
    try {
      CubeMetastoreClient metastore = CubeMetastoreClient.getInstance(new HiveConf(conf, CubeSelectorService.class));
      allCubes = metastore.getAllCubes();
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }


  @Override
  public Map<Cube, List<String>> selectCubes(Collection<String> columns) {
    Map<Cube, List<String>> selection = new HashMap<Cube, List<String>>();

    // find all matching cubes
    for (Cube cube : allCubes) {
      for (String column : columns) {
        if (cube.getMeasureByName(column) != null || cube.getDimensionByName(column) != null) {
          List<String> subset = selection.get(cube);
          if (subset == null) {
            subset = new ArrayList<String>();
            selection.put(cube, subset);
          }
          subset.add(column);
        }
      }
    }

    // Group cubes by column subset and paths
    Map<String, List<Cube>> cubeGroup = new HashMap<String,  List<Cube>>();
    for (Map.Entry<Cube, List<String>> entry : selection.entrySet()) {
      Cube cube = entry.getKey();
      List<String> subset = entry.getValue();

      StringBuilder buf = new StringBuilder();
      for (String column : subset) {
        buf.append(column);
      }

      String groupKey =
        buf.append(cube.getProperties().get("cube." + cube.getName().replace("cube_", "") + ".path")).toString();

      List<Cube> group = cubeGroup.get(groupKey);
      if (group == null) {
        group = new ArrayList<Cube>();
        cubeGroup.put(groupKey, group);
      }
      group.add(cube);
    }

    // In each group retain only the min cost cube
    for (List<Cube> group : cubeGroup.values()) {
      Cube minCostCube = group.get(0);
      String costKey = "cube." + minCostCube.getName().replace("cube_", "") + ".cost";
      String costStr = minCostCube.getProperties().get(costKey);
      int minCost = costStr == null ? Integer.MIN_VALUE : Integer.valueOf(costStr);
      for (int i = 1; i < group.size(); i++) {
        Cube cube = group.get(i);
        costKey = "cube." + cube.getName().replace("cube_", "") + ".cost";
        costStr = cube.getProperties().get(costKey);
        int cubeCost = costStr == null ? Integer.MIN_VALUE : Integer.valueOf(costStr);
        if (cubeCost < minCost) {
          minCostCube = cube;
          minCost = cubeCost;
        }
      }

      // Remove cubes with higher costs and same paths from selection
      for (Cube cube : group) {
        if (cube != minCostCube) {
          selection.remove(cube);
        }
      }
    }

    return selection;
  }
}
