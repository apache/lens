package org.apache.lens.client;

/**
 * Top level builder class for lens connection.
 */
public class LensConnectionBuilder {

  /** The base url. */
  private String baseUrl;

  /** The database. */
  private String database;

  /** The user. */
  private String user;

  /** The password. */
  private String password;

  /**
   * Instantiates a new lens connection builder.
   */
  public LensConnectionBuilder() {
  }

  /**
   * Base url.
   *
   * @param baseUrl
   *          the base url
   * @return the lens connection builder
   */
  public LensConnectionBuilder baseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
    return this;
  }

  /**
   * Database.
   *
   * @param database
   *          the database
   * @return the lens connection builder
   */
  public LensConnectionBuilder database(String database) {
    this.database = database;
    return this;
  }

  /**
   * User.
   *
   * @param user
   *          the user
   * @return the lens connection builder
   */
  public LensConnectionBuilder user(String user) {
    this.user = user;
    return this;
  }

  /**
   * Password.
   *
   * @param password
   *          the password
   * @return the lens connection builder
   */
  public LensConnectionBuilder password(String password) {
    this.password = password;
    return this;
  }

  /**
   * Builds the.
   *
   * @return the lens connection
   */
  public LensConnection build() {
    LensConnectionParams params = new LensConnectionParams();
    if (baseUrl != null && !baseUrl.isEmpty()) {
      params.setBaseUrl(baseUrl);
    }
    if (database != null && !database.isEmpty()) {
      params.setDbName(database);
    }
    if (user != null && !user.isEmpty()) {
      params.getSessionVars().put("user.name", user);
    }
    if (password != null && !password.isEmpty()) {
      params.getSessionVars().put("user.pass", password);
    }
    return new LensConnection(params);
  }

}
