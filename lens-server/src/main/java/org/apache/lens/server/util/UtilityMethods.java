/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.util;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.alerts.Email;
import org.apache.lens.server.api.metrics.MetricsService;

import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * The Class UtilityMethods.
 */
public class UtilityMethods {

  private static final String EMAIL_ERROR_COUNTER = "email-send-errors";

  public static final Log LOG = LogFactory.getLog(UtilityMethods.class);

  /**
   * Merge maps.
   *
   * @param <K>
   *          the key type
   * @param <V>
   *          the value type
   * @param into
   *          the into
   * @param from
   *          the from
   * @param override
   *          the override
   */
  public static <K, V> void mergeMaps(Map<K, V> into, Map<K, V> from, boolean override) {
    for (K key : from.keySet()) {
      if (override || !into.containsKey(key)) {
        into.put(key, from.get(key));
      }
    }
  }

  /**
   * Removes the domain.
   *
   * @param username
   *          the username
   * @return the string
   */
  public static String removeDomain(String username) {
    if (username.contains("@")) {
      username = username.substring(0, username.indexOf("@"));
    }
    return username;
  }

  /**
   * Any null.
   *
   * @param args
   *          the args
   * @return true, if successful
   */
  public static boolean anyNull(Object... args) {
    for (Object arg : args) {
      if (arg == null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Query database.
   *
   * @param ds
   *          the ds
   * @param querySql
   *          the query sql
   * @param allowNull
   *          the allow null
   * @param args
   *          the args
   * @return the string[]
   * @throws SQLException
   *           the SQL exception
   */
  public static String[] queryDatabase(DataSource ds, String querySql, final boolean allowNull, Object... args)
      throws SQLException {
    QueryRunner runner = new QueryRunner(ds);
    return runner.query(querySql, new ResultSetHandler<String[]>() {
      @Override
      public String[] handle(ResultSet resultSet) throws SQLException {
        String[] result = new String[resultSet.getMetaData().getColumnCount()];
        if (!resultSet.next()) {
          if (allowNull) {
            return null;
          }
          throw new SQLException("no rows retrieved in query");
        }
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
          result[i - 1] = resultSet.getString(i);
        }
        if (resultSet.next()) {
          throw new SQLException("more than one row retrieved in query");
        }
        return result;
      }
    }, args);
  }

  /**
   * Gets the data source from conf.
   *
   * @param conf
   *          the conf
   * @return the data source from conf
   */
  public static BasicDataSource getDataSourceFromConf(Configuration conf) {
    BasicDataSource tmp = new BasicDataSource();
    tmp.setDriverClassName(conf.get(LensConfConstants.SERVER_DB_DRIVER_NAME,
        LensConfConstants.DEFAULT_SERVER_DB_DRIVER_NAME));
    tmp.setUrl(conf.get(LensConfConstants.SERVER_DB_JDBC_URL, LensConfConstants.DEFAULT_SERVER_DB_JDBC_URL));
    tmp.setUsername(conf.get(LensConfConstants.SERVER_DB_JDBC_USER, LensConfConstants.DEFAULT_SERVER_DB_USER));
    tmp.setPassword(conf.get(LensConfConstants.SERVER_DB_JDBC_PASS, LensConfConstants.DEFAULT_SERVER_DB_PASS));
    return tmp;
  }

  /**
   * Conf to string.
   *
   * @param conf
   *          the conf
   * @return the string
   */
  public static String confToString(Configuration conf) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : conf) {
      sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
    }
    return sb.toString();
  }

  /**
   * Send mail.
   *
   * @param host
   *          the host
   * @param port
   *          the port
   * @param email
   *          the mail message
   * @param mailSmtpTimeout
   *          the mail smtp timeout
   * @param mailSmtpConnectionTimeout
   *          the mail smtp connection timeout
   */
  public static void sendMail(String host, String port, String from, Email email, int mailSmtpTimeout, int mailSmtpConnectionTimeout) {
    Properties props = System.getProperties();
    props.put("mail.smtp.host", host);
    props.put("mail.smtp.port", port);
    props.put("mail.smtp.timeout", mailSmtpTimeout);
    props.put("mail.smtp.connectiontimeout", mailSmtpConnectionTimeout);
    Session session = Session.getDefaultInstance(props, null);
    try {
      MimeMessage message = new MimeMessage(session);
      message.setFrom(new InternetAddress(from));
      for(String recipient: email.getTo().trim().split("\\s*,\\s*")) {
        message.addRecipients(Message.RecipientType.TO, InternetAddress.parse(recipient));
      }
      if (email.getCc() != null && !email.getCc().isEmpty()) {
        for(String recipient: email.getCc().trim().split("\\s*,\\s*")) {
          message.addRecipients(Message.RecipientType.CC, InternetAddress.parse(recipient));
        }
      }
      if(email.getBcc() != null && !email.getBcc().isEmpty()) {
        for(String recipient: email.getBcc().trim().split("\\s*,\\s*")) {
          message.addRecipients(Message.RecipientType.BCC, InternetAddress.parse(recipient));
        }
      }
      message.setSubject(email.getSubject());
      message.setSentDate(new Date());

      MimeBodyPart messagePart = new MimeBodyPart();
      messagePart.setText(email.getMessage());
      Multipart multipart = new MimeMultipart();

      multipart.addBodyPart(messagePart);
      message.setContent(multipart);
      Transport.send(message);
    } catch (Exception e) {
      MetricsService metricsService = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      metricsService.incrCounter(UtilityMethods.class, EMAIL_ERROR_COUNTER);
      LOG.error("Error sending mail", e);
    }
  }
}

