package com.inmobi.grill.driver.api;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.query.QueryPrepareHandle;

import lombok.Getter;
import lombok.Setter;

public class PreparedQueryContext implements Delayed {
  @Getter private final QueryPrepareHandle prepareHandle;
  @Getter private final String userQuery;
  @Getter private final Date preparedTime;
  @Getter private final String preparedUser;
  @Getter private final Configuration conf;
  @Getter @Setter private GrillDriver selectedDriver;
  @Getter @Setter private String driverQuery;

  private static long millisInWeek = 7 * 24 * 60 * 60 * 1000;

  public PreparedQueryContext(String query, String user, Configuration conf) {
    this.userQuery = query;
    this.preparedTime = new Date();
    this.preparedUser = user;
    this.prepareHandle = new QueryPrepareHandle(UUID.randomUUID());
    this.conf = conf;
    this.driverQuery = query;
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

  /**
   * @param conf the conf to set
   */
  public void updateConf(Map<String,String> confoverlay) {
    for (Map.Entry<String,String> prop : confoverlay.entrySet()) {
      this.conf.set(prop.getKey(), prop.getValue());
    }
  }

  public com.inmobi.grill.query.GrillPreparedQuery toPreparedQuery() {
    // TODO Auto-generated method stub
    return null;
  }

}
