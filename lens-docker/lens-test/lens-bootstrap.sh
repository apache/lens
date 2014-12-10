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
echo "LENS_CLIENT " $LENS_CLIENT

#start hive bootstrap script
/etc/hive-bootstrap.sh

echo "Waiting for 10 secs for servers to start ..."
sleep 10

#start lens server
echo "Starting Lens server..."
$LENS_HOME/bin/lens-ctl start

echo "Waiting for 20 secs for Lens Server to start ..."
sleep 20

#Setting up client
$LENS_CLIENT/bin/run-examples.sh sample-metastore
$LENS_CLIENT/bin/run-examples.sh populate-metastore
$LENS_CLIENT/bin/lens-cli.sh

/bin/bash