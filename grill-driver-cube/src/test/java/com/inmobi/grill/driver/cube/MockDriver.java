package com.inmobi.grill.driver.cube;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.GrillResultSetMetadata;
import com.inmobi.grill.api.PreparedQueryContext;
import com.inmobi.grill.api.QueryCompletionListener;
import com.inmobi.grill.api.QueryContext;
import com.inmobi.grill.api.QueryCost;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;

public class MockDriver implements GrillDriver {

  Configuration conf;
  public MockDriver() {    
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void configure(Configuration conf) throws GrillException {
    this.conf = conf;
  }

  static class MockQueryPlan extends QueryPlan {
    String query;
    MockQueryPlan(String query) {
      this.query = query;
      setPrepareHandle(new QueryPrepareHandle(UUID.randomUUID()));
    }
    @Override
    public String getPlan() {
      return query;
    }

    @Override
    public QueryCost getCost() {
      return new QueryCost(0L, 0.0);
    }
  }

  @Override
  public QueryPlan explain(String query, Configuration conf)
      throws GrillException {
    return new MockQueryPlan(query);
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle) throws GrillException {
    return new QueryStatus(1.0, QueryStatus.Status.SUCCESSFUL, "Done", false);
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws GrillException {
    return false;
  }

  @Override
  public void closeQuery(QueryHandle handle) throws GrillException {
  }

  @Override
  public void close() throws GrillException {
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public QueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException {
    QueryPlan p = new MockQueryPlan(pContext.getDriverQuery());
    p.setPrepareHandle(pContext.getPrepareHandle());
    return p;
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle)
      throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public GrillResultSet execute(QueryContext context) throws GrillException {
    return new GrillResultSet() {

      @Override
      public int size() throws GrillException {
        return 0;
      }

      @Override
      public GrillResultSetMetadata getMetadata() throws GrillException {
        return new GrillResultSetMetadata() {

          @Override
          public List<Column> getColumns() {
            return new ArrayList<Column>();
          }
        };
      }
    };
  }

  @Override
  public void executeAsync(QueryContext context) throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public GrillResultSet fetchResultSet(QueryContext context)
      throws GrillException {
    return new GrillResultSet() {

      @Override
      public int size() throws GrillException {
        return 0;
      }

      @Override
      public GrillResultSetMetadata getMetadata() throws GrillException {
        return new GrillResultSetMetadata() {

          @Override
          public List<Column> getColumns() {
            return new ArrayList<Column>();
          }
        };
      }
    };
  }

  @Override
  public void closeResultSet(QueryHandle handle) throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void registerForCompletionNotification(QueryHandle handle,
      long timeoutMillis, QueryCompletionListener listener)
      throws GrillException {
    // TODO Auto-generated method stub
    
  }

}
