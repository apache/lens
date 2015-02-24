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

ENV LENS_VERSION apache-lens-2.1.0-beta-incubating-SNAPSHOT
ENV BASEDIR /opt/lens
ENV LENS_HOME $BASEDIR/lens-dist/target/${LENS_VERSION}-bin/${LENS_VERSION}-bin/server
ENV LENS_CLIENT $BASEDIR/lens-dist/target/${LENS_VERSION}-bin/${LENS_VERSION}-bin/client

ENV LENS_SERVER_CONF $LENS_HOME/conf-pseudo-distr/
ENV LENS_CLIENT_CONF $LENS_CLIENT/conf-pseudo-distr/


# set permissions for lens bootstrap file
ADD lens-bootstrap.sh /etc/lens-bootstrap.sh
RUN chown root:root /etc/lens-bootstrap.sh
RUN chmod 700 /etc/lens-bootstrap.sh

CMD ["/etc/lens-bootstrap.sh"]

