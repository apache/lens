#!/bin/bash
#
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


################################
# constants
################################

CLIENT_DIMENSIONDDL_CLASS="com.inmobi.yoda.cube.ddl.DimensionDDL"
CLIENT_CUBEDDL_CLASS="com.inmobi.yoda.cube.ddl.CubeDDL"

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


display_help() {
  cat <<EOF
Usage: $0 dimensionddl --conf <confdir>
       $0 cubeddl [cubename] --conf <confdir>
EOF
}

run_client() {
  local CLIENT_APPLICATION_CLASS

  if [ "$#" -gt 0 ]; then
    CLIENT_APPLICATION_CLASS=$1
    shift
  else
    error "Must specify client application class" 1
  fi

  set -x
  exec $JAVA_HOME/bin/java $JAVA_OPTS -cp "$CLIENT_CLASSPATH" \
      "$CLIENT_APPLICATION_CLASS" $*
}

################################
# main
################################

# set default params
CLIENT_CLASSPATH=""
CLIENT_JAVA_LIBRARY_PATH=""
JAVA_OPTS="-Xmx128M"

opt_conf=""

mode=$1
shift

case "$mode" in
  help)
    display_help
    exit 0
    ;;
  dimensionddl)
    opt_dim=1
    ;;
  cubeddl)
    opt_cube=1
    ;;
  *)
    error "Unknown or unspecified command '$mode'"
    echo
    display_help
    exit 1
    ;;
esac

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
      CLIENT_CLASSPATH=$1
      shift
      ;;
    -D*)
      JAVA_OPTS="${JAVA_OPTS} $arg"
      ;;
    *)
      args="$args $arg"
      ;;
  esac
done


# find java
if [ -z "${JAVA_HOME}" ] ; then
  echo "Warning: JAVA_HOME not set!"
    JAVA_DEFAULT=`type -p java`
    [ -n "$JAVA_DEFAULT" ] || error "Unable to find java executable. Is it in your PATH?" 1
    JAVA_HOME=$(cd $(dirname $JAVA_DEFAULT)/..; pwd)
fi

[ -n "${JAVA_HOME}" ] || error "Unable to find a suitable JAVA_HOME" 1

# figure out where the client distribution is
if [ -z "${CLIENT_HOME}" ] ; then
  CLIENT_HOME=$(cd $(dirname $0)/..; pwd)
fi
#echo CLIENT_HOME is $CLIENT_HOME

# Append to the classpath
if [ -n "${CLIENT_CLASSPATH}" ] ; then
  CLIENT_CLASSPATH="${CLIENT_HOME}/lib/*:$DEV_CLASSPATH:$CLIENT_CLASSPATH"
else
  CLIENT_CLASSPATH="${CLIENT_HOME}/lib/*:$DEV_CLASSPATH"
fi

# prepend conf dir to classpath
if [ -n "$opt_conf" ]; then
  CLIENT_CLASSPATH="$opt_conf:$CLIENT_CLASSPATH"
fi

# finally, invoke the appropriate command
if [ -n "$opt_dim" ] ; then
  run_client $CLIENT_DIMENSIONDDL_CLASS $args
elif [ -n "$opt_cube" ] ; then
  run_client $CLIENT_CUBEDDL_CLASS $args
else
  error "This message should never appear" 1
fi
exit 0
