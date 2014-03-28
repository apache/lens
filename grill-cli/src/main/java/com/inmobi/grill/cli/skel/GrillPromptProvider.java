package com.inmobi.grill.cli.skel;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultPromptProvider;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class GrillPromptProvider extends DefaultPromptProvider {


  @Override
  public String getPrompt() {
    return "grill-shell>";
  }


  @Override
  public String name() {
    return "grill prompt provider";
  }
}
