package com.inmobi.grill.server.scheduler;

import org.apache.hive.service.cli.CLIService;

import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.api.scheduler.QuerySchedulerService;

public class QuerySchedulerServiceImpl extends GrillService implements QuerySchedulerService {

  public QuerySchedulerServiceImpl(CLIService cliService) {
    super("scheduler", cliService);
  }
}
