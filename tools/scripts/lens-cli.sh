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
. ${BASEDIR}/bin/lens-config.sh 'client'

JAVA_PROPERTIES="$LENS_OPTS $LENS_PROPERTIES -Dlens.log.dir=$LENS_LOG_DIR -Dlens.home=${LENS_HOME_DIR} -Dconfig.location=$LENS_CONF"

################################
# constants
################################

CLI_JAR="org.springframework.shell.Bootstrap"

################################
# functions
################################

info() {
  local msg=$1

  echo "Info: $msg" >&2
}

warn() {
  local msg=$1

  echo "Warning: $msg" >&2
}

error() {
  local msg=$1
  local exit_code=$2

  echo "Error: $msg" >&2

  if [ -n "$exit_code" ] ; then
    exit $exit_code
  fi
}


################################
# main
################################

opt_conf=""

while [ -n "$*" ] ; do
  arg=$1
  shift

  case "$arg" in
    --conf|-c)
      [ -n "$1" ] || error "Option --conf requires an argument" 1
      opt_conf=$1
      shift
      ;;
    --classpath|-C)
      [ -n "$1" ] || error "Option --classpath requires an argument" 1
      LENSCPPATH="LENSCPPATH:$1"
      shift
      ;;
    -D*)
      JAVA_PROPERTIES="${JAVA_PROPERTIES} $arg"
      ;;
    *)
      args="$args $arg"
      ;;
  esac
done



# prepend conf dir to classpath
if [ -n "$opt_conf" ]; then
  LENSCPPATH="$opt_conf:$LENSCPPATH"
fi

# finally, invoke the appropriate command

exec ${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${LENSCPPATH} \
      "$CLI_JAR" $*

exit 0
