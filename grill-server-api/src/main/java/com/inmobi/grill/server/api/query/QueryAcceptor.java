package com.inmobi.grill.server.api.query;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.SubmitOp;

public interface QueryAcceptor {

  /**
   * Whether to accept the query or not
   * 
   * @param query The query
   * @param conf The configuration of the query
   *
   * @return null if query should be accepted, rejection cause otherwise
   * 
   * @throws GrillException
   */
  public String accept(String query, Configuration conf, SubmitOp submitOp) throws GrillException;

}
