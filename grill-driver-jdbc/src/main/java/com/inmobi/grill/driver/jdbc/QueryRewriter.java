package com.inmobi.grill.driver.jdbc;


import com.inmobi.grill.api.GrillException;
import org.apache.hadoop.conf.Configuration;

public interface QueryRewriter {
  public String rewrite(Configuration conf, String query) throws GrillException;
}
