package com.inmobi.grill.server.api.driver;

import java.util.List;

import com.inmobi.grill.api.query.QueryResultSetMetadata;
import com.inmobi.grill.api.query.ResultColumn;

public abstract class GrillResultSetMetadata {

  public abstract List<ResultColumn> getColumns();

  public QueryResultSetMetadata toQueryResultSetMetadata() {
    return new QueryResultSetMetadata(getColumns());
  }
}
