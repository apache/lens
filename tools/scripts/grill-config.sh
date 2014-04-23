#!/bin/bash
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

if [ -z "$GRILL_CONF" ]; then
  GRILL_CONF=${BASEDIR}/conf
fi
export GRILL_CONF

if [ -f "${GRILL_CONF}/grill-env.sh" ]; then
  . "${GRILL_CONF}/grill-env.sh"
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
GRILL_OPTS="$DEFAULT_JAVA_HEAP_MAX $GRILL_OPTS"

type="$1"
shift
case $type in
  client)
    # set the client class path
    GRILLCPPATH="$GRILL_CONF:${BASEDIR}/lib/*"
    GRILL_OPTS="$GRILL_OPTS $GRILL_CLIENT_OPTS $GRILL_CLIENT_HEAP"
    GRILL_LOG_DIR="${GRILL_LOG_DIR:-$BASEDIR/logs}"
    export GRILL_LOG_DIR    
    GRILL_HOME_DIR="${GRILL_HOME_DIR:-$BASEDIR}"
    export GRILL_HOME_DIR    
  ;;
  server)
    GRILL_OPTS="$GRILL_OPTS $GRILL_SERVER_OPTS $GRILL_SERVER_HEAP"
    GRILLCPPATH="$GRILL_CONF" 
    GRILL_EXPANDED_WEBAPP_DIR=${GRILL_EXPANDED_WEBAPP_DIR:-${BASEDIR}/webapp}
    export GRILL_EXPANDED_WEBAPP_DIR
    # set the server classpath
    if [ ! -d ${GRILL_EXPANDED_WEBAPP_DIR}/grill-server/WEB-INF ]; then
      mkdir -p ${GRILL_EXPANDED_WEBAPP_DIR}/grill-server
      cd ${GRILL_EXPANDED_WEBAPP_DIR}/grill-server
      $JAR_BIN -xf ${BASEDIR}/webapp/grill-server.war
      cd -
    fi
    GRILLCPPATH="${GRILLCPPATH}:${GRILL_EXPANDED_WEBAPP_DIR}/grill-server/WEB-INF/classes"
    GRILLCPPATH="${GRILLCPPATH}:${GRILL_EXPANDED_WEBAPP_DIR}/grill-server/WEB-INF/lib/*:${BASEDIR}/lib/*"

    HADOOP_CLASSPATH="$HADOOP_CLASSPATH:${GRILL_EXPANDED_WEBAPP_DIR}/grill-server/WEB-INF/lib/*"
    export HADOOP_CLASSPATH
    
    # log and pid dirs for applications
    GRILL_LOG_DIR="${GRILL_LOG_DIR:-$BASEDIR/logs}"
    export GRILL_LOG_DIR
    GRILL_PID_DIR="${GRILL_PID_DIR:-$BASEDIR/logs}"
    # create the pid dir if its not there
    [ -w "$GRILL_PID_DIR" ] ||  mkdir -p "$GRILL_PID_DIR"
    export GRILL_PID_DIR
    GRILL_PID_FILE=${GRILL_PID_DIR}/${grill-server}.pid
    export GRILL_PID_FILE
    GRILL_DATA_DIR=${GRILL_DATA_DIR:-${BASEDIR}/data}
    GRILL_HOME_DIR="${GRILL_HOME_DIR:-$BASEDIR}"
    export GRILL_HOME_DIR
  ;;
  *)
    echo "Invalid option for type: $type"
    exit 1
  ;;
esac
export GRILLCPPATH
export GRILL_OPTS
