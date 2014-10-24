package org.apache.lens.cli.skel;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultPromptProvider;
import org.springframework.stereotype.Component;

/**
 * The Class LensPromptProvider.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class LensPromptProvider extends DefaultPromptProvider {

  @Override
  public String getPrompt() {
    return "lens-shell>";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.springframework.shell.plugin.support.DefaultPromptProvider#name()
   */
  @Override
  public String name() {
    return "lens prompt provider";
  }
}
