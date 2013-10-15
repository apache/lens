package com.inmobi.grill.driver.cube;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillDriver;

public interface DriverSelector {
  public GrillDriver select(List<GrillDriver> drivers,
      Map<GrillDriver, String> queries, Configuration conf);
}
