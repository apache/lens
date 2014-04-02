package com.inmobi.grill.server.quota;

import org.apache.hive.service.cli.CLIService;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.api.quota.QuotaService;

public class QuotaServiceImpl extends GrillService implements QuotaService {

  public QuotaServiceImpl(CLIService cliService) {
    super("quota", cliService);
  }

}
