package com.inmobi.grill.cli.skel;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultHistoryFileNameProvider;
import org.springframework.stereotype.Component;


@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class GrillHistoryFileProvider extends DefaultHistoryFileNameProvider {

  public String getHistoryFileName() {
    return "grill-cli-hist.log";
  }

  @Override
  public String name() {
    return "grill client history provider";
  }
}
