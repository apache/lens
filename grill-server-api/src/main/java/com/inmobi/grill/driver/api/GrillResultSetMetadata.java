package com.inmobi.grill.driver.api;

import java.util.List;

import com.inmobi.grill.query.QueryResultSetMetadata;
import com.inmobi.grill.query.ResultColumn;

public abstract class GrillResultSetMetadata {

  public abstract List<ResultColumn> getColumns();

  public QueryResultSetMetadata toQueryResultSetMetadata() {
    return new QueryResultSetMetadata(getColumns());
  }
}
