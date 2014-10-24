package org.apache.lens.server.scheduler;

import org.apache.hive.service.cli.CLIService;
import org.apache.lens.server.LensService;
import org.apache.lens.server.api.scheduler.QuerySchedulerService;

/**
 * The Class QuerySchedulerServiceImpl.
 */
public class QuerySchedulerServiceImpl extends LensService implements QuerySchedulerService {

  /**
   * Instantiates a new query scheduler service impl.
   *
   * @param cliService
   *          the cli service
   */
  public QuerySchedulerServiceImpl(CLIService cliService) {
    super("scheduler", cliService);
  }
}
