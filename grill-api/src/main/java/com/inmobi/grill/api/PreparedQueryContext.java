package com.inmobi.grill.api;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

public class PreparedQueryContext implements Delayed {
  private final String userQuery;
  private final Date preparedTime;
  private final String preparedUser;
  private final Configuration conf;
  private final QueryPrepareHandle prepareHandle;
  private GrillDriver selectedDriver;
  private String driverQuery;

  private static long millisInWeek = 7 * 24 * 60 * 60 * 1000;

  public PreparedQueryContext(String query, String user, Configuration conf) {
    this.userQuery = query;
    this.preparedTime = new Date();
    this.preparedUser = user;
    this.prepareHandle = new QueryPrepareHandle(UUID.randomUUID());
    this.conf = conf;
    this.driverQuery = query;
  }

  /**
   * @return the userQuery
   */
  public String getUserQuery() {
    return userQuery;
  }

  /**
   * @return the preparedTime
   */
  public Date getPreparedTime() {
    return preparedTime;
  }

  /**
   * @return the preparedUser
   */
  public String getPreparedUser() {
    return preparedUser;
  }

  /**
   * @return the conf
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * @return the prepareHandle
   */
  public QueryPrepareHandle getPrepareHandle() {
    return prepareHandle;
  }

  /**
   * @return the selectedDriver
   */
  public GrillDriver getSelectedDriver() {
    return selectedDriver;
  }

  /**
   * @param selectedDriver the selectedDriver to set
   */
  public void setSelectedDriver(GrillDriver selectedDriver) {
    this.selectedDriver = selectedDriver;
  }

  /**
   * @return the driverQuery
   */
  public String getDriverQuery() {
    return driverQuery;
  }

  /**
   * @param driverQuery the driverQuery to set
   */
  public void setDriverQuery(String driverQuery) {
    this.driverQuery = driverQuery;
  }

  @Override
  public int compareTo(Delayed o) {
    return (int)(this.getDelay(TimeUnit.MILLISECONDS)
        - o.getDelay(TimeUnit.MILLISECONDS));
  }

  @Override
  public long getDelay(TimeUnit units) {
    long delayMillis;
    if (this.preparedTime != null) {
      Date now = new Date();
      long elapsedMills = now.getTime() - this.preparedTime.getTime();
      delayMillis = millisInWeek - elapsedMills;
      return units.convert(delayMillis, TimeUnit.MILLISECONDS);
    } else {
      return Integer.MAX_VALUE;
    }
  }

}
