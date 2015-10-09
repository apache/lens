#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM inmobi/docker-hive

RUN wget http://apache.mirrors.lucidnetworks.net/spark/spark-1.3.0/spark-1.3.0-bin-hadoop2.4.tgz
RUN gunzip spark-1.3.0-bin-hadoop2.4.tgz
RUN tar -xvf spark-1.3.0-bin-hadoop2.4.tar
RUN mv spark-1.3.0-bin-hadoop2.4 /usr/local
ENV SPARK_HOME /usr/local/spark-1.3.0-bin-hadoop2.4
RUN rm spark-1.3.0-bin-hadoop2.4.tar

ENV LENS_VERSION 2.5.0-beta-SNAPSHOT
ENV BASEDIR /opt/lens
ENV LENS_HOME $BASEDIR/lens-dist/target/apache-lens-${LENS_VERSION}-bin/apache-lens-${LENS_VERSION}-bin/server
ENV LENS_CLIENT $BASEDIR/lens-dist/target/apache-lens-${LENS_VERSION}-bin/apache-lens-${LENS_VERSION}-bin/client

ENV LENS_SERVER_CONF $LENS_HOME/conf-pseudo-distr/
ENV LENS_CLIENT_CONF $LENS_CLIENT/conf-pseudo-distr/

ENV LENS_ML $BASEDIR/lens-ml-dist/target/apache-lens-${LENS_VERSION}-ml/


# set permissions for lens bootstrap file
ADD lens-bootstrap.sh /etc/lens-bootstrap.sh
RUN chown root:root /etc/lens-bootstrap.sh
RUN chmod 700 /etc/lens-bootstrap.sh

CMD ["/etc/lens-bootstrap.sh"]

