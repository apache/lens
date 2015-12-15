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

import static org.apache.lens.server.api.LensConfConstants.*;

import java.util.Date;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryEnded;
import org.apache.lens.server.model.LogSegregationContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class QueryEndNotifier.
 */
@Slf4j
public class QueryEndNotifier extends AsyncEventListener<QueryEnded> {

  /** The query service. */
  private final QueryExecutionServiceImpl queryService;

  /** The Constant EMAIL_ERROR_COUNTER. */
  public static final String EMAIL_ERROR_COUNTER = "email-send-errors";

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

  private final LogSegregationContext logSegregationContext;

  /** QueryEndNotifier core and max pool size */
  private static final int CORE_POOL_SIZE = 2;
  private static final int MAX_POOL_SIZE = 5;

  /** Instantiates a new query end notifier.
   *
   * @param queryService the query service
   * @param hiveConf     the hive conf */
  public QueryEndNotifier(QueryExecutionServiceImpl queryService, HiveConf hiveConf,
    @NonNull final LogSegregationContext logSegregationContext) {
    super(CORE_POOL_SIZE, MAX_POOL_SIZE);
    this.queryService = queryService;
    HiveConf conf = hiveConf;
    from = conf.get(MAIL_FROM_ADDRESS);
    host = conf.get(MAIL_HOST);
    port = conf.get(MAIL_PORT);
    mailSmtpTimeout = Integer.parseInt(conf.get(MAIL_SMTP_TIMEOUT, MAIL_DEFAULT_SMTP_TIMEOUT));
    mailSmtpConnectionTimeout = Integer.parseInt(conf.get(MAIL_SMTP_CONNECTIONTIMEOUT,
      MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT));
    this.logSegregationContext = logSegregationContext;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent) */
  @Override
  public void process(final QueryEnded event) {
    if (event.getCurrentValue() == QueryStatus.Status.CLOSED) {
      return;
    }
    QueryContext queryContext = event.getQueryContext();
    if (queryContext == null) {
      log.warn("Could not find the context for {} for event:{}. No email generated", event.getQueryHandle(),
        event.getCurrentValue());
      return;
    }
    this.logSegregationContext.setLogSegragationAndQueryId(queryContext.getQueryHandleString());

    boolean whetherMailNotify = Boolean.parseBoolean(queryContext.getConf().get(QUERY_MAIL_NOTIFY,
      WHETHER_MAIL_NOTIFY_DEFAULT));
    if (!whetherMailNotify) {
      return;
    }

    try {
      //Create and Send EMAIL
      String queryName = queryContext.getQueryName();
      String mailSubject = "Query " + (StringUtils.isBlank(queryName) ? "" : (queryName + " "))
        + queryContext.getStatus().getStatus() + ": " + event.getQueryHandle();

      String mailMessage = createMailMessage(queryContext);

      String to = queryContext.getSubmittedUser() + "@" + queryService.getServerDomain();

      String cc = queryContext.getConf().get(QUERY_RESULT_EMAIL_CC, QUERY_RESULT_DEFAULT_EMAIL_CC);

      log.info("Sending completion email for query handle: {}", event.getQueryHandle());
      sendMail(host, port, new Email(from, to, cc, mailSubject, mailMessage), mailSmtpTimeout,
          mailSmtpConnectionTimeout);
    } catch (Exception e) {
      MetricsService metricsService = LensServices.get().getService(MetricsService.NAME);
      metricsService.incrCounter(QueryEndNotifier.class, EMAIL_ERROR_COUNTER);
      log.error("Error sending query end email", e);
    }
  }

  /** Creates the mail message.
   *
   * @param queryContext the query context
   * @return the string */
  private String createMailMessage(QueryContext queryContext) {
    StringBuilder msgBuilder = new StringBuilder();
    switch (queryContext.getStatus().getStatus()) {
    case SUCCESSFUL:
      msgBuilder.append(getResultMessage(queryContext));
      break;
    case FAILED:
      msgBuilder.append(queryContext.getStatus().getStatusMessage());
      if (!StringUtils.isBlank(queryContext.getStatus().getErrorMessage())) {
        msgBuilder.append("\n Reason:\n");
        msgBuilder.append(queryContext.getStatus().getErrorMessage());
      }
      break;
    case CANCELED:
      msgBuilder.append(queryContext.getStatus().getStatusMessage());
      break;
    default:
      break;
    }
    return msgBuilder.toString();
  }

  private String getResultMessage(QueryContext queryContext) {
    try {
      return queryService.getResultset(queryContext.getQueryHandle()).toQueryResult().toPrettyString();
    } catch (LensException e) {
      log.error("Error retrieving result of query handle {} for sending e-mail", queryContext.getQueryHandle(), e);
      return "Error retrieving result.";
    }
  }

  @Data
  public static class Email {
    private final String from;
    private final String to;
    private final String cc;
    private final String subject;
    private final String message;
  }

  /** Send mail.
   *
   * @param host                      the host
   * @param port                      the port
   * @param email                     the email
   * @param mailSmtpTimeout           the mail smtp timeout
   * @param mailSmtpConnectionTimeout the mail smtp connection timeout */
  public static void sendMail(String host, String port,
    Email email, int mailSmtpTimeout, int mailSmtpConnectionTimeout) throws Exception{
    Properties props = System.getProperties();
    props.put("mail.smtp.host", host);
    props.put("mail.smtp.port", port);
    props.put("mail.smtp.timeout", mailSmtpTimeout);
    props.put("mail.smtp.connectiontimeout", mailSmtpConnectionTimeout);
    Session session = Session.getDefaultInstance(props, null);
    MimeMessage message = new MimeMessage(session);
    message.setFrom(new InternetAddress(email.getFrom()));
    for (String recipient : email.getTo().trim().split("\\s*,\\s*")) {
      message.addRecipients(Message.RecipientType.TO, InternetAddress.parse(recipient));
    }
    if (email.getCc() != null && email.getCc().length() > 0) {
      for (String recipient : email.getCc().trim().split("\\s*,\\s*")) {
        message.addRecipients(Message.RecipientType.CC, InternetAddress.parse(recipient));
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
  }
}
