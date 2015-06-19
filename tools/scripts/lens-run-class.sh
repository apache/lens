#!/usr/bin/env bash
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

setenv() {

# HADOOP_HOME env variable overrides hadoop in the path
  HADOOP_HOME=${HADOOP_HOME:-${HADOOP_PREFIX}}
  if [ "$HADOOP_HOME" == "" ]; then
    echo "Cannot find hadoop installation: \$HADOOP_HOME or \$HADOOP_PREFIX must be set or hadoop must be in the path";
    exit 4;
  else
    echo "Adding hadoop libs in classpath from $HADOOP_HOME"

    #ASSUMPTION: hadoop jars would be present in HADOOP_HOME if installed through deb or would be present
    # in HADOOP_HOME/share/hadoop if installed through tarball. They can not coexist.

    CORE_JARS=`ls $HADOOP_HOME/hadoop-core-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`

    LIB_JARS=`ls $HADOOP_HOME/lib/guava-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`
    LIB_JARS=$LIB_JARS:`ls ${HADOOP_HOME}/lib/commons-configuration-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`
    LIB_JARS=$LIB_JARS:`ls ${HADOOP_HOME}/lib/protobuf-java-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`
    LIB_JARS=$LIB_JARS:`ls ${HADOOP_HOME}/share/hadoop/common/lib/commons-configuration-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`
    LIB_JARS=$LIB_JARS:`ls ${HADOOP_HOME}/share/hadoop/hdfs/lib/protobuf-java-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`


    COMMON_JARS=`ls ${HADOOP_HOME}/hadoop-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`
    COMMON_JARS=$COMMON_JARS:`ls ${HADOOP_HOME}/share/hadoop/common/hadoop-common-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`
    COMMON_JARS=$COMMON_JARS:`ls ${HADOOP_HOME}/share/hadoop/common/lib/hadoop-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`

    HDFS_JARS=`ls ${HADOOP_HOME}/../hadoop-hdfs/hadoop-hdfs-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`
    HDFS_JARS=$HDFS_JARS:`ls ${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`

    MAPRED_JARS=`ls ${HADOOP_HOME}/../hadoop-mapreduce/hadoop-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`
    MAPRED_JARS=$MAPRED_JARS:`ls ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-*.jar 2>/dev/null | tr "\n" ':' 2>/dev/null`

    HADOOP_JARPATH=$CORE_JARS:$LIB_JARS:$COMMON_JARS:$HDFS_JARS:$MAPRED_JARS
    LENSCPPATH=${LENSCPPATH}:$HADOOP_JARPATH
  fi

  if [ "$HIVE_HOME" != "" ]; then
    echo "HIVE_HOME is set, adding ${HIVE_HOME}/lib/* into lens classpath"
    LENSCPPATH=${LENSCPPATH}:`ls ${HIVE_HOME}/lib/* 2>/dev/null | tr "\n" ':' 2>/dev/null`
  else
    echo "HIVE_HOME is not set. Set HIVE_HOME and try again"
    exit 1
  fi

  # Add HIVE_HOME to HADOOP_CLASS_PATH
  HADOOP_CLASSPATH="$HADOOP_CLASSPATH:${HIVE_HOME}/lib/*"
  export HADOOP_CLASSPATH

}
################################
# main
################################

opt_conf=""
opt_classname=""
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
      if [ "$opt_classname" == "" ]; then
        opt_classname=$arg
        echo "opt_classname is " $opt_classname
      else
        args="$args $arg"
      fi
      ;;
  esac
done
echo "args are  " $args


# prepend conf dir to classpath
if [ -n "$opt_conf" ]; then
  LENSCPPATH="$opt_conf:$LENSCPPATH"
fi

# finally, invoke the appropriate command
if [ "$opt_classname" == "" ]; then
  echo "Usage : $0 <classname>"
  exit 1
fi
echo "Executing class " $opt_classname

setenv

exec ${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${LENSCPPATH} \
      "$opt_classname" $args

exit 0
