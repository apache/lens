package com.inmobi.grill.client;

/**
 * Top level builder class for grill connection
 */
public class GrillConnectionBuilder {

  private String hostName;
  private int portNumber=-1;
  private String database;
  private String user;
  private String password;

  public GrillConnectionBuilder(String hostName) {
    this.hostName = hostName;
  }

  public GrillConnectionBuilder port(int portNumber) {
    this.portNumber = portNumber;
    return this;
  }

  public GrillConnectionBuilder database(String database) {
    this.database = database;
    return this;
  }

  public GrillConnectionBuilder user(String user) {
    this.user = user;
    return this;
  }

  public GrillConnectionBuilder password(String password) {
    this.password = password;
    return this;
  }


  public GrillConnection build() {
    GrillConnectionParams params = new GrillConnectionParams();
    if(hostName != null && !hostName.isEmpty()){
      params.setHost(hostName);
    }
    if(portNumber != -1) {
      params.setPort(portNumber);
    }
    if(database != null && !database.isEmpty()) {
      params.setDbName(database);
    }
    if (user != null && !user.isEmpty()) {
      params.getSessionVars().put("user.name", user);
    }
    if (password != null && !password.isEmpty()) {
      params.getSessionVars().put("user.pass", password);
    }
    return new GrillConnection(params);
  }

}
