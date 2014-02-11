package com.inmobi.grill.server.api.query;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.Priority;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.GrillDriver;

import lombok.Getter;
import lombok.Setter;

public class QueryContext implements Comparable<QueryContext> {

  @Getter @Setter private QueryHandle queryHandle;
  @Getter final private String userQuery;
  @Getter final private Date submissionTime;
  @Getter final private String submittedUser;
  @Getter private Configuration conf;
  @Getter private Priority priority;
  @Getter final private boolean isPersistent;
  @Getter @Setter private GrillDriver selectedDriver;
  @Getter @Setter private String driverQuery;
  @Getter @Setter private QueryStatus status;
  @Getter @Setter private String resultSetPath;
  @Getter @Setter private long cancelTime;
  @Getter @Setter private long closedTime;
  @Getter @Setter private long endTime;
  @Getter @Setter private long launchTime;
  @Getter @Setter private long runningTime;
  @Getter @Setter private String grillSessionIdentifier;
  @Getter @Setter private String driverOpHandle;

  public QueryContext(String query, String user, Configuration conf) {
    this.userQuery = query;
    this.submissionTime = new Date();
    this.submittedUser = user;
    this.queryHandle = new QueryHandle(UUID.randomUUID());
    this.priority = Priority.NORMAL;
    this.conf = conf;
    this.isPersistent = conf.getBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, true);
    this.driverQuery = query;
  }

  public QueryContext(PreparedQueryContext prepared, String user,
      Configuration conf) {
    this.userQuery = prepared.getUserQuery();
    this.submissionTime = new Date();
    this.submittedUser = user;
    this.queryHandle = new QueryHandle(UUID.randomUUID());
    this.priority = Priority.NORMAL;
    this.conf = mergeConf(prepared.getConf(), conf);
    this.isPersistent = this.conf.getBoolean(
        GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, true);
    this.driverQuery = prepared.getDriverQuery();
    this.selectedDriver = prepared.getSelectedDriver();
  }

  private Configuration mergeConf(Configuration prepared,
      Configuration current) {
    Configuration conf = new Configuration(prepared);
    for (Map.Entry<String, String> entry : current) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  @Override
  public int compareTo(QueryContext other) {
    int pcomp = this.priority.compareTo(other.priority);
    if (pcomp == 0) {
      return this.submissionTime.compareTo(other.submissionTime);
    } else {
      return pcomp;
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

  public String getResultSetPersistentPath() {
    if (isPersistent) {
      return conf.get(GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR);
    }
    return null;
  }

  public GrillQuery toGrillQuery() {
    GrillConf qconf = new GrillConf();
    for (Map.Entry<String, String> p : conf) {
      qconf.addProperty(p.getKey(), p.getValue());
    }
    return new GrillQuery(queryHandle, userQuery, submissionTime,
        submittedUser, priority, isPersistent,
        selectedDriver != null ? selectedDriver.getClass().getCanonicalName() : null,
        driverQuery, status, resultSetPath, driverOpHandle, qconf);
  }
}