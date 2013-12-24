package com.inmobi.grill.queryscheduler.service;

import org.apache.hive.service.cli.CLIService;

import com.inmobi.grill.server.api.GrillService;
import com.inmobi.grill.server.api.QuerySchedulerService;

public class QuerySchedulerServiceImpl extends GrillService implements QuerySchedulerService {

  public QuerySchedulerServiceImpl(CLIService cliService) {
    super("scheduler", cliService);
  }
}
