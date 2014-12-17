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
package org.apache.lens.server.alerts;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.alerts.Alertable;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.events.LensEvent;
import org.apache.lens.server.util.UtilityMethods;

public class EmailHandler<T extends LensEvent & Alertable> extends AsyncEventListener<T> {
  public static final Log LOG = LogFactory.getLog(EmailHandler.class);
  private final HiveConf conf;
  private final boolean whetherMail;
  private final boolean whetherLog;
  private final String from;
  private final String host;
  private final String port;
  private final int mailSmtpTimeout;
  private final int mailSmtpConnectionTimeout;

  public EmailHandler(HiveConf conf, boolean whetherLog, boolean whetherMail) {
    this.conf = conf;
    this.whetherLog = whetherLog;
    this.whetherMail = whetherMail;
    from = conf.get(LensConfConstants.MAIL_FROM_ADDRESS);
    host = conf.get(LensConfConstants.MAIL_HOST);
    port = conf.get(LensConfConstants.MAIL_PORT);
    mailSmtpTimeout = Integer.parseInt(conf.get(LensConfConstants.MAIL_SMTP_TIMEOUT,
      LensConfConstants.MAIL_DEFAULT_SMTP_TIMEOUT));
    mailSmtpConnectionTimeout = Integer.parseInt(conf.get(LensConfConstants.MAIL_SMTP_CONNECTIONTIMEOUT,
      LensConfConstants.MAIL_DEFAULT_SMTP_CONNECTIONTIMEOUT));
  }

  @Override
  public void process(T event) {
    if (whetherLog) {
      LOG.error(event.getLogMessage());
    }
    if (whetherMail) {
      UtilityMethods.sendMail(host, port, from, null, null, event.getEmailSubject(), event.getEmailMessage(),
        mailSmtpTimeout, mailSmtpConnectionTimeout);
    }
  }
}
