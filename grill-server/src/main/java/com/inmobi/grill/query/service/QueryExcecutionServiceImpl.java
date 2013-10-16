package com.inmobi.grill.query.service;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.QueryExecutionService;
import com.inmobi.grill.server.api.QueryHandleWithResultSet;

public class QueryExcecutionServiceImpl implements QueryExecutionService {

  @Override
  public QueryPlan explain(String query, Configuration conf)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void executePrepareAsync(QueryHandle handle, Configuration conf)
      throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public QueryHandle executeAsync(String query, Configuration conf)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryHandleWithResultSet execute(String query, long timeoutmillis,
      Configuration conf) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GrillResultSet fetchResultSet(QueryHandle handle)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<QueryHandle> getAllQueries(Status status) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void start() throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void stop() throws GrillException {
    // TODO Auto-generated method stub
    
  }

}
