package org.apache.lens.cli.skel;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultHistoryFileNameProvider;
import org.springframework.stereotype.Component;

/**
 * The Class LensHistoryFileProvider.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class LensHistoryFileProvider extends DefaultHistoryFileNameProvider {

  public String getHistoryFileName() {
    return "lens-cli-hist.log";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.springframework.shell.plugin.support.DefaultHistoryFileNameProvider#name()
   */
  @Override
  public String name() {
    return "lens client history provider";
  }
}
