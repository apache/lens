package com.inmobi.grill.query.service;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.exception.GrillException;

public interface QueryAcceptor {

  /**
   * Whether to accept the query or not
   * 
   * @param query The query
   * @param conf The configuration of the query
   * @param cause Populate cause especially in case of returning false
   * 
   * @return true if query should be accepted, false otherwise
   * 
   * @throws GrillException
   */
  public boolean doAccept(String query, Configuration conf,
      final String cause) throws GrillException;

}
