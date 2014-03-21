package com.inmobi.grill.client;


import com.inmobi.grill.api.GrillConf;

import java.util.HashMap;
import java.util.Map;

/**
 * Top level class which encapsulates connections parameters required for grill
 * connection.
 */
public class GrillConnectionParams {

  private Map<String,String> grillConfs = new HashMap<String, String>();
  private Map<String,String> grillVars = new HashMap<String, String>();
  private Map<String,String> sessionVars = new HashMap<String, String>();

  private final GrillClientConfig conf;

  /**
   * Construct parameters required to connect to grill server using values in
   * grill-client-site.xml
   */
  public GrillConnectionParams() {
    this.conf = new GrillClientConfig();
  }

  /**
   * Construct parameters required to connect to grill server from values passed
   * in configuration.
   *
   * @param conf from which connection parameters are defined.
   */
  public GrillConnectionParams(GrillClientConfig conf) {
    this.conf = conf;
  }

  /**
   * Gets host to which grill client should connect to.
   * @return hostname of grill server
   */
  public String getHost() {
    return conf.getGrillHost();
  }

  /**
   * Gets the port to which grill client should connect to.
   * @return portnumber of grill server
   */
  public int getPort() {
    return conf.getGrillPort();
  }

  /**
   * Gets the Database to which grill client should connect to.
   * @return database to connect to
   */
  public String getDbName() {
    return conf.getGrillDatabase();
  }


  public Map<String, String> getGrillConfs() {
    return grillConfs;
  }

  public Map<String, String> getGrillVars() {
    return grillVars;
  }

  public Map<String, String> getSessionVars() {
    return sessionVars;
  }

  public void setHost(String host) {
    this.conf.setGrillHost(host);
  }

  public void setPort(int port) {
    this.conf.setGrillPort(port);
  }

  public void setDbName(String dbName) {
    this.conf.setGrillDatabase(dbName);
  }


  public String getBaseConnectionUrl() {
    return "http://"+this.getHost()+":"
        +this.getPort()+"/"
        +this.conf.getAppBasePath();
  }

  public GrillClientConfig getConf() {
    return this.conf;
  }

  public String getUser() {
    return this.sessionVars.get("user.name")!= null ?
        this.sessionVars.get("user.name") : "";
  }

  public String getPassword() {
    return this.sessionVars.get("user.password") != null?
        this.sessionVars.get("user.password") : "";
  }

  public String getSessionResourcePath() {
    return this.conf.getSessionResourcePath();
  }

  public String getQueryResourcePath() {
    return this.conf.getQueryResourcePath();
  }

  public String getMetastoreResourcePath() {
    return this.conf.getMetastoreResourcePath();
  }

  public long getQueryPollInterval() {
    return this.conf.getQueryPollInterval();
  }


  public GrillConf getSessionConf() {
    GrillConf conf = new GrillConf();
    for(Map.Entry<String, String> entry: grillConfs.entrySet()) {
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    for(Map.Entry<String, String> entry: sessionVars.entrySet()) {
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    for(Map.Entry<String, String> entry : grillVars.entrySet()) {
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GrillConnectionParams{");
    sb.append("dbName='").append(this.conf.getGrillDatabase()).append('\'');
    sb.append(", host='").append(this.conf.getGrillHost()).append('\'');
    sb.append(", port=").append(this.conf.getGrillPort());
    sb.append(", grillConfs=").append(grillConfs);
    sb.append(", grillVars=").append(grillVars);
    sb.append(", sessionVars=").append(sessionVars);
    sb.append('}');
    return sb.toString();
  }
}
