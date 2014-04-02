package com.inmobi.grill.client;

import com.inmobi.grill.api.query.QueryResult;
import com.inmobi.grill.api.query.QueryResultSetMetadata;

public class GrillClientResultSet {

  private final QueryResult result;
  private final QueryResultSetMetadata resultSetMetadata;

  public GrillClientResultSet(QueryResult result,
                              QueryResultSetMetadata resultSetMetaData) {
    this.result = result;
    this.resultSetMetadata = resultSetMetaData;
  }

  public QueryResult getResult() {
    return result;
  }

  public QueryResultSetMetadata getResultSetMetadata() {
    return resultSetMetadata;
  }
}
