#!/bin/bash

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

#start hive bootstrap script
/etc/hive-bootstrap.sh

echo "Waiting for 20 secs for servers to start ..."
sleep 20

#start lens server
echo "Starting Lens server..."
$LENS_HOME/bin/lens-ctl start --conf $LENS_SERVER_CONF 

echo "Waiting for 40 secs for Lens Server to start ..."
sleep 40

#Setting up client
$LENS_CLIENT/bin/run-examples.sh sample-metastore --conf $LENS_CLIENT_CONF
$LENS_CLIENT/bin/run-examples.sh populate-metastore --conf $LENS_CLIENT_CONF
$LENS_CLIENT/bin/lens-cli.sh --conf $LENS_CLIENT_CONF

/bin/bash