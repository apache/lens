package com.inmobi.grill.es.cube.join;

import java.util.List;
/**
 * Immutable object representing the join config passed as argument to the query.
 */
public class JoinConfig {
  final String table;
  final String joinedColumn;
  final String factColumn;
  final String joinConditionStr;
  final String joinType;
  final List<String> columnsToGet;

  public JoinConfig(String table, String joinedColumn, String factColumn, String joinType,
      String joinConditionStr, List<String> colsToGet) {
    this.table = table;
    this.joinedColumn = joinedColumn;
    this.factColumn = factColumn;
    this.joinConditionStr = joinConditionStr;
    this.columnsToGet = colsToGet;
    this.joinType = joinType;
  }

  public String getTable() {
    return table;
  }

  public String getJoinedColumn() {
    return joinedColumn;
  }

  public String getFactColumn() {
    return factColumn;
  }

  public String getJoinConditionStr() {
    return joinConditionStr;
  }

  public String getJoinType() {
    return joinType;
  }

  public List<String> getColumnsToGet() {
    return columnsToGet;
  }
  
  
}
