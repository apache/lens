package org.apache.lens.cli.skel;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

/**
 * The Class LensBanner.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class LensBanner extends DefaultBannerProvider {

  @Override
  public String getBanner() {
    StringBuffer buf = new StringBuffer();
    buf.append("=======================================" + OsUtils.LINE_SEPARATOR);
    buf.append("*                                     *" + OsUtils.LINE_SEPARATOR);
    buf.append("*            Lens Client              *" + OsUtils.LINE_SEPARATOR);
    buf.append("*                                     *" + OsUtils.LINE_SEPARATOR);
    buf.append("=======================================" + OsUtils.LINE_SEPARATOR);
    return buf.toString();

  }

  @Override
  public String getWelcomeMessage() {
    return "Welcome to Lens Client";
  }
}
