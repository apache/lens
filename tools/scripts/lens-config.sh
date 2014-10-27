#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

if [ -z "$LENS_CONF" ]; then
  LENS_CONF=${BASEDIR}/conf
fi
export LENS_CONF

if [ -f "${LENS_CONF}/lens-env.sh" ]; then
  . "${LENS_CONF}/lens-env.sh"
fi

if test -z ${JAVA_HOME}
then
    JAVA_BIN=`which java`
    JAR_BIN=`which jar`
else
    JAVA_BIN=${JAVA_HOME}/bin/java
    JAR_BIN=${JAVA_HOME}/bin/jar
fi
export JAVA_BIN

if [ ! -e $JAVA_BIN ] || [ ! -e $JAR_BIN ]; then
  echo "$JAVA_BIN and/or $JAR_BIN not found on the system. Please make sure java and jar commands are available."
  exit 1
fi

# default the heap size to 1GB
DEFAULT_JAVA_HEAP_MAX=-Xmx1024m
LENS_OPTS="$DEFAULT_JAVA_HEAP_MAX $LENS_OPTS"

type="$1"
shift
case $type in
  client)
    # set the client class path
    LENSCPPATH="$LENS_CONF:${BASEDIR}/lib/*"
    LENS_OPTS="$LENS_OPTS $LENS_CLIENT_OPTS $LENS_CLIENT_HEAP"
    LENS_LOG_DIR="${LENS_LOG_DIR:-$BASEDIR/logs}"
    export LENS_LOG_DIR    
    LENS_HOME_DIR="${LENS_HOME_DIR:-$BASEDIR}"
    export LENS_HOME_DIR    
  ;;
  server)
    LENS_OPTS="$LENS_OPTS $LENS_SERVER_OPTS $LENS_SERVER_HEAP"
    LENSCPPATH="$LENS_CONF" 
    LENS_EXPANDED_WEBAPP_DIR=${LENS_EXPANDED_WEBAPP_DIR:-${BASEDIR}/webapp}
    export LENS_EXPANDED_WEBAPP_DIR
    # set the server classpath
    if [ ! -d ${LENS_EXPANDED_WEBAPP_DIR}/lens-server/WEB-INF ]; then
      mkdir -p ${LENS_EXPANDED_WEBAPP_DIR}/lens-server
      cd ${LENS_EXPANDED_WEBAPP_DIR}/lens-server
      $JAR_BIN -xf ${BASEDIR}/webapp/lens-server.war
      cd -
    fi
    LENSCPPATH="${LENSCPPATH}:${LENS_EXPANDED_WEBAPP_DIR}/lens-server/WEB-INF/classes"
    LENSCPPATH="${LENSCPPATH}:${LENS_EXPANDED_WEBAPP_DIR}/lens-server/WEB-INF/lib/*:${BASEDIR}/lib/*"

    HADOOP_CLASSPATH="$HADOOP_CLASSPATH:${LENS_EXPANDED_WEBAPP_DIR}/lens-server/WEB-INF/lib/*"
    export HADOOP_CLASSPATH
    
    # log and pid dirs for applications
    LENS_LOG_DIR="${LENS_LOG_DIR:-$BASEDIR/logs}"
    export LENS_LOG_DIR
    LENS_PID_DIR="${LENS_PID_DIR:-$LENS_LOG_DIR}"
    # create the pid dir if its not there
    [ -w "$LENS_PID_DIR" ] ||  mkdir -p "$LENS_PID_DIR"
    export LENS_PID_DIR
    LENS_PID_FILE=${LENS_PID_DIR}/${lens-server}.pid
    export LENS_PID_FILE
    LENS_DATA_DIR=${LENS_DATA_DIR:-${BASEDIR}/data}
    LENS_HOME_DIR="${LENS_HOME_DIR:-$BASEDIR}"
    export LENS_HOME_DIR
  ;;
  *)
    echo "Invalid option for type: $type"
    exit 1
  ;;
esac
export LENSCPPATH
export LENS_OPTS
