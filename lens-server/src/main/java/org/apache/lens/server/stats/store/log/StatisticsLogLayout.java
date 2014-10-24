package org.apache.lens.server.stats.store.log;

import org.apache.log4j.SimpleLayout;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Empty statistics log layout for log4j.
 */
public class StatisticsLogLayout extends SimpleLayout {

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.log4j.SimpleLayout#format(org.apache.log4j.spi.LoggingEvent)
   */
  @Override
  public String format(LoggingEvent event) {
    return event.getRenderedMessage() + LINE_SEP;
  }
}
