package com.inmobi.grill.server.api.driver;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public interface DriverSelector {
  public GrillDriver select(List<GrillDriver> drivers,
      Map<GrillDriver, String> queries, Configuration conf);
}
