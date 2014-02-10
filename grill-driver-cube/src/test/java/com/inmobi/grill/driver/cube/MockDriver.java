package com.inmobi.grill.driver.cube;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.driver.api.GrillDriver;
import com.inmobi.grill.driver.api.GrillResultSet;
import com.inmobi.grill.driver.api.GrillResultSetMetadata;
import com.inmobi.grill.driver.api.InMemoryResultSet;
import com.inmobi.grill.driver.api.PersistentResultSet;
import com.inmobi.grill.driver.api.PreparedQueryContext;
import com.inmobi.grill.driver.api.QueryCompletionListener;
import com.inmobi.grill.driver.api.QueryContext;
import com.inmobi.grill.driver.api.DriverQueryPlan;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.query.QueryCost;
import com.inmobi.grill.query.QueryHandle;
import com.inmobi.grill.query.QueryPrepareHandle;
import com.inmobi.grill.query.QueryResult;
import com.inmobi.grill.query.QueryResultSetMetadata;
import com.inmobi.grill.query.QueryStatus;
import com.inmobi.grill.query.ResultColumn;
import com.inmobi.grill.query.ResultRow;

public class MockDriver implements GrillDriver {

  Configuration conf;
  String query;

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

  static class MockQueryPlan extends DriverQueryPlan {
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
  public DriverQueryPlan explain(String query, Configuration conf)
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
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException {
    DriverQueryPlan p = new MockQueryPlan(pContext.getDriverQuery());
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
    this.query = context.getDriverQuery();
    return new PersistentResultSet() {
      
      @Override
      public int size() throws GrillException {
        // TODO Auto-generated method stub
        return 0;
      }
      
      @Override
      public GrillResultSetMetadata getMetadata() throws GrillException {
        // TODO Auto-generated method stub
        return new GrillResultSetMetadata() {
          
          @Override
          public List<ResultColumn> getColumns() {
            // TODO Auto-generated method stub
            return null;
          }
        };
      }
      
      @Override
      public String getOutputPath() throws GrillException {
        // TODO Auto-generated method stub
        return null;
      }
    };
  }

  @Override
  public void executeAsync(QueryContext context) throws GrillException {
    this.query = context.getDriverQuery();
  }

  @Override
  public GrillResultSet fetchResultSet(QueryContext context)
      throws GrillException {
    return new InMemoryResultSet() {
      
      @Override
      public int size() throws GrillException {
        // TODO Auto-generated method stub
        return 0;
      }
      
      @Override
      public GrillResultSetMetadata getMetadata() throws GrillException {
        return new GrillResultSetMetadata() {
          
          @Override
          public List<ResultColumn> getColumns() {
            // TODO Auto-generated method stub
            return null;
          }
        };
      }
      
      @Override
      public void setFetchSize(int size) throws GrillException {
        // TODO Auto-generated method stub
        
      }
      
      @Override
      public ResultRow next() throws GrillException {
        // TODO Auto-generated method stub
        return null;
      }
      
      @Override
      public boolean hasNext() throws GrillException {
        // TODO Auto-generated method stub
        return false;
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
