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
package org.apache.lens.server.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryEnded;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.Date;
import java.util.Properties;

/**
 * The Class QueryEndNotifier.
 */
public class QueryEndNotifier extends AsyncEventListener<QueryEnded> {

  /** The query service. */
  private final QueryExecutionServiceImpl queryService;

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(QueryEndNotifier.class);

  /** The Constant EMAIL_ERROR_COUNTER. */
  public static final String EMAIL_ERROR_COUNTER = "email-send-errors";

  /** The conf. */
  private final HiveConf conf;

  /** The from. */
  private final String from;

  /** The host. */
  private final String host;

  /** The port. */
  private final String port;

  /** The mail smtp timeout. */
  private final int mailSmtpTimeout;

  /** The mail smtp connection timeout. */
  private final int mailSmtpConnectionTimeout;

  /**
   * Instantiates a new query end notifier.
   *
   * @param queryService
   *          the query service
   * @param hiveConf
   *          the hive conf
   */
  public QueryEndNotifier(QueryExecutionServiceImpl queryService, HiveConf hiveConf) {
    this.queryService = queryService;
    this.conf = hiveConf;
    from = conf.get(LensConfConstants.MAIL_FROM_ADDRESS);
    host = conf.get(LensConfConstants.MAIL_HOST);
    port = conf.get(LensConfConstants.MAIL_PORT);
    mailSmtpTimeout = Integer.parseInt(conf.get(LensConfConstants.MAIL_SMTP_TIMEOUT,
        LensConfConstants.MAIL_DEFAULT_SMTP_TIMEOUT));
    mailSmtpConnectionTimeout = Integer.parseInt(conf.get(LensConfConstants.MAIL_SMTP_CONNECTIONTIMEOUT,
        LensConfConstants.MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT));

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
   */
  @Override
  public void process(QueryEnded event) {
    if (event.getCurrentValue() == QueryStatus.Status.CLOSED) {
      return;
    }
    QueryContext queryContext = queryService.getQueryContext(event.getQueryHandle());
    if (queryContext == null) {
      LOG.warn("Could not find the context for " + event.getQueryHandle() + " for event:" + event.getCurrentValue()
          + ". No email generated");
      return;
    }

    boolean whetherMailNotify = Boolean.parseBoolean(queryContext.getConf().get(LensConfConstants.QUERY_MAIL_NOTIFY,
        LensConfConstants.WHETHER_MAIL_NOTIFY_DEFAULT));

    if (!whetherMailNotify) {
      return;
    }

    String queryName = queryContext.getQueryName();
    queryName = queryName == null ? "" : queryName;
    String mailSubject = "Query " + queryName + " " + queryContext.getStatus().getStatus() + ": "
        + event.getQueryHandle();

    String mailMessage = createMailMessage(queryContext);

    String to = queryContext.getSubmittedUser() + "@" + queryService.getServerDomain();

    String cc = queryContext.getConf().get(LensConfConstants.QUERY_RESULT_EMAIL_CC,
        LensConfConstants.QUERY_RESULT_DEFAULT_EMAIL_CC);

    LOG.info("Sending completion email for query handle: " + event.getQueryHandle());
    sendMail(host, port, from, to, cc, mailSubject, mailMessage, mailSmtpTimeout, mailSmtpConnectionTimeout);
  }

  /**
   * Creates the mail message.
   *
   * @param queryContext
   *          the query context
   * @return the string
   */
  private String createMailMessage(QueryContext queryContext) {
    StringBuilder msgBuilder = new StringBuilder();
    switch (queryContext.getStatus().getStatus()) {
    case SUCCESSFUL:
      msgBuilder.append("Result available at ");
      String baseURI = conf.get(LensConfConstants.SERVER_BASE_URL, LensConfConstants.DEFAULT_SERVER_BASE_URL);
      msgBuilder.append(baseURI);
      msgBuilder.append("queryapi/queries/");
      msgBuilder.append(queryContext.getQueryHandle());
      msgBuilder.append("/httpresultset");
      break;
    case FAILED:
      msgBuilder.append(queryContext.getStatus().getErrorMessage());
      break;
    case CANCELED:
    case CLOSED:
    default:
      break;
    }
    return msgBuilder.toString();
  }

  /**
   * Send mail.
   *
   * @param host
   *          the host
   * @param port
   *          the port
   * @param from
   *          the from
   * @param to
   *          the to
   * @param cc
   *          the cc
   * @param subject
   *          the subject
   * @param mailMessage
   *          the mail message
   * @param mailSmtpTimeout
   *          the mail smtp timeout
   * @param mailSmtpConnectionTimeout
   *          the mail smtp connection timeout
   */
  public static void sendMail(String host, String port, String from, String to, String cc, String subject,
      String mailMessage, int mailSmtpTimeout, int mailSmtpConnectionTimeout) {
    Properties props = System.getProperties();
    props.put("mail.smtp.host", host);
    props.put("mail.smtp.port", port);
    props.put("mail.smtp.timeout", mailSmtpTimeout);
    props.put("mail.smtp.connectiontimeout", mailSmtpConnectionTimeout);
    Session session = Session.getDefaultInstance(props, null);
    try {
      MimeMessage message = new MimeMessage(session);
      message.setFrom(new InternetAddress(from));
      message.setRecipient(Message.RecipientType.TO, new InternetAddress(to));
      if (cc != null && cc.length() > 0) {
        message.setRecipient(Message.RecipientType.CC, new InternetAddress(cc));
      }
      message.setSubject(subject);
      message.setSentDate(new Date());

      MimeBodyPart messagePart = new MimeBodyPart();
      messagePart.setText(mailMessage);
      Multipart multipart = new MimeMultipart();

      multipart.addBodyPart(messagePart);
      message.setContent(multipart);
      Transport.send(message);
    } catch (Exception e) {
      MetricsService metricsService = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      metricsService.incrCounter(QueryEndNotifier.class, EMAIL_ERROR_COUNTER);
      LOG.error("Error sending query end email", e);
    }
  }
}
