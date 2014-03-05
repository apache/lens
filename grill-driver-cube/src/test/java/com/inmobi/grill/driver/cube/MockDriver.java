package com.inmobi.grill.driver.cube;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.GrillResultSetMetadata;
import com.inmobi.grill.api.QueryCost;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;

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

  static class MockQueryPlan extends QueryPlan {
    String query;
    MockQueryPlan(String query) {
      this.query = query;
      setHandle(new QueryHandle(UUID.randomUUID()));
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
  public GrillResultSet executePrepare(QueryHandle handle, Configuration conf)
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
  public void executePrepareAsync(QueryHandle handle, Configuration conf)
      throws GrillException {
  }

  @Override
  public GrillResultSet execute(String query, Configuration conf)
      throws GrillException {
    this.query = query;
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
  public QueryHandle executeAsync(String query, Configuration conf)
      throws GrillException {
    this.query = query;
    return new QueryHandle(UUID.randomUUID());
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle) throws GrillException {
    return new QueryStatus(1.0, QueryStatus.Status.SUCCESSFUL, "Done", false);
  }

  @Override
  public GrillResultSet fetchResultSet(QueryHandle handle)
      throws GrillException {
    return null;
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
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // TODO Auto-generated method stub
    
  }

}
