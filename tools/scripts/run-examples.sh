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

CLIENT_METASTORE_CLASS="org.apache.lens.examples.SampleMetastore"
CLIENT_POPULATE_CLASS="org.apache.lens.examples.PopulateSampleMetastore"
CLIENT_QUERY_CLASS="org.apache.lens.examples.SampleQueries"

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
Usage: $0 sample-metastore [-db dbname] --conf <confdir>
       $0 populate-metastore [-db dbname] --conf <confdir>
       $0 runqueries [-db dbname] --conf <confdir>
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
  exec ${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${LENSCPPATH} \
      "$CLIENT_APPLICATION_CLASS" $*
}

################################
# main
################################

opt_conf=""

mode=$1
shift

case "$mode" in
  help)
    display_help
    exit 0
    ;;
  sample-metastore)
    opt_meta=1
    ;;
  populate-metastore)
    opt_populate=1
    ;;
  runqueries)
    opt_query=1
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
      LENSCPPATH=$1
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

# prepend resources dir to classpath
LENSCPPATH="${LENS_HOME_DIR}/examples/resources:${LENS_HOME_DIR}/examples/queries:$LENSCPPATH"

# finally, invoke the appropriate command
if [ -n "$opt_meta" ] ; then
  run_client $CLIENT_METASTORE_CLASS $args
elif [ -n "$opt_populate" ] ; then
  run_client $CLIENT_POPULATE_CLASS $args
elif [ -n "$opt_query" ] ; then
  run_client $CLIENT_QUERY_CLASS $args
else
  error "This message should never appear" 1
fi
exit 0
