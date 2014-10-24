package org.apache.lens.server.quota;

import org.apache.hive.service.cli.CLIService;
import org.apache.lens.server.LensService;
import org.apache.lens.server.api.quota.QuotaService;

/**
 * The Class QuotaServiceImpl.
 */
public class QuotaServiceImpl extends LensService implements QuotaService {

  /**
   * Instantiates a new quota service impl.
   *
   * @param cliService
   *          the cli service
   */
  public QuotaServiceImpl(CLIService cliService) {
    super("quota", cliService);
  }

}
