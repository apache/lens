package com.inmobi.grill.cli.skel;


import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class GrillBanner extends DefaultBannerProvider {

  @Override
  public String getBanner() {
    StringBuffer buf = new StringBuffer();
    buf.append("=======================================" +
        OsUtils.LINE_SEPARATOR);
    buf.append("*                                     *"+ OsUtils.LINE_SEPARATOR);
    buf.append("*            GrillClient               *" +OsUtils.LINE_SEPARATOR);
    buf.append("*                                     *"+ OsUtils.LINE_SEPARATOR);
    buf.append("=======================================" + OsUtils.LINE_SEPARATOR);
    return buf.toString();

  }

  @Override
  public String getWelcomeMessage() {
    return "Welcome to Grill Client";
  }
}
