package org.apache.lens.server.stats;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.lens.server.stats.event.LoggableLensStatistics;
import org.apache.lens.server.stats.store.log.LogStatisticsStore;
import org.apache.lens.server.stats.store.log.StatisticsLogLayout;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.StringWriter;

/**
 * The Class TestLogStatisticsStore.
 */
public class TestLogStatisticsStore {

  /**
   * The Class MyLoggableLens.
   */
  private static class MyLoggableLens extends LoggableLensStatistics {

    /**
     * Instantiates a new my loggable lens.
     *
     * @param eventTime
     *          the event time
     */
    public MyLoggableLens(long eventTime) {
      super(eventTime);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.server.stats.event.LoggableLensStatistics#getHiveTable(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public Table getHiveTable(Configuration conf) {
      return null;
    }

    @Override
    public String getEventId() {
      return "random";
    }
  }

  /**
   * Test log statistics store.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testLogStatisticsStore() throws Exception {
    LogStatisticsStore store = new LogStatisticsStore();
    store.initialize(new Configuration());
    // Do some initialization work
    StringWriter writer = new StringWriter();
    Logger l = Logger.getLogger(MyLoggableLens.class);
    WriterAppender appender = new WriterAppender(new StatisticsLogLayout(), writer);

    appender.setName(MyLoggableLens.class.getSimpleName());
    l.addAppender(appender);
    MyLoggableLens sampleEvent = new MyLoggableLens(System.currentTimeMillis());
    store.process(sampleEvent);
    writer.flush();
    l.removeAppender(appender);
    ObjectMapper mapper = new ObjectMapper();
    String expected = mapper.writeValueAsString(sampleEvent);
    Assert.assertEquals(writer.toString().trim(), expected.trim());
  }
}
