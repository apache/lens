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
. ${BASEDIR}/bin/grill-config.sh 'server'

# make sure the process is not running
if [ -f $GRILL_PID_FILE ]; then
  if kill -0 `cat $GRILL_PID_FILE` > /dev/null 2>&1; then
    echo grill-server running as process `cat $GRILL_PID_FILE`.  Stop it first.
    exit 1
  fi
fi

mkdir -p $GRILL_LOG_DIR

pushd ${BASEDIR} > /dev/null

JAVA_PROPERTIES="$GRILL_OPTS $GRILL_PROPERTIES -Dgrill.log.dir=$GRILL_LOG_DIR -Dgrill.home=${GRILL_HOME_DIR} -Dconfig.location=$GRILL_CONF"
shift

while [[ ${1} =~ ^\-D ]]; do
  JAVA_PROPERTIES="${JAVA_PROPERTIES} ${1}"
  shift
done
TIME=`date +%Y%m%d%H%M%s`

nohup ${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${GRILLCPPATH} com.inmobi.grill.server.GrillServer $* > "${GRILL_LOG_DIR}/grill-server.out.$TIME" 2>&1 < /dev/null &
echo $! > $GRILL_PID_FILE
popd > /dev/null

echo Started grill server!
