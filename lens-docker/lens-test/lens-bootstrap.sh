#!/usr/bin/env bash

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

echo "LENS_HOME " $LENS_HOME
echo "LENS_SERVER_CONF " $LENS_SERVER_CONF
echo "LENS_CLIENT " $LENS_CLIENT
echo "LENS_CLIENT_CONF " $LENS_CLIENT_CONF
echo "LENS_ML " $LENS_ML
echo "SPARK_HOME " $SPARK_HOME

#do the clean
rm $LENS_HOME/*.log
rm $LENS_HOME/logs/*
rm -rf $LENS_HOME/metastore_db

#set ml classpath into LENS_EXT_CLASSPATH
LENS_EXT_CLASSPATH=$LENS_EXT_CLASSPATH:`$LENS_ML/bin/lens-ml-classpath.sh`
export LENS_EXT_CLASSPATH

SPARK_YARN_JAR=$SPARK_HOME/lib/spark-assembly-1.3.0-hadoop2.4.0.jar
export SPARK_YARN_JAR
echo "SPARK_YARN_JAR " $SPARK_YARN_JAR

HIVE_AUX_JARS_PATH=$LENS_ML/lib/lens-ml-lib-2.7.0-SNAPSHOT.jar,$SPARK_YARN_JAR
export HIVE_AUX_JARS_PATH

echo "HIVE_AUX_JARS_PATH " $HIVE_AUX_JARS_PATH

#start hive bootstrap script
/etc/hive-bootstrap.sh

checkservice() {
  servicename=$1
  port=$2
  up=`netstat -plnt |grep :$port.*LISTEN | wc -l`
  while [ $up != "1" ]
  do
    echo "$servicename not yet up at $port. waiting for 5 sec"
    sleep 5
    up=`netstat -plnt |grep :$port.*LISTEN | wc -l`
  done
  echo "$servicename up at $port"
}

checkservice "Namenode" 9000
checkservice "RM" 8088
checkservice "HiveMetastore" 9083
checkservice "HiveServer" 10000

#start lens server
echo "Starting Lens server..."
$LENS_HOME/bin/lens-ctl start --conf $LENS_SERVER_CONF

checkservice "LensServer" 9999

#Setting up client
$LENS_CLIENT/bin/run-examples.sh sample-metastore --conf $LENS_CLIENT_CONF
$LENS_CLIENT/bin/run-examples.sh populate-metastore --conf $LENS_CLIENT_CONF
$LENS_CLIENT/bin/lens-cli.sh --conf $LENS_CLIENT_CONF

/usr/bin/env bash
