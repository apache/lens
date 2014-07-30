#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# The java implementation to use. If JAVA_HOME is not found we expect java and jar to be in path
#export JAVA_HOME=

# The hadoop installation location. If Grill is configured to run queries through Hive, which in turn uses Hadoop for execution, the variable is required
#export HADOOP_HOME=

# The Hive installation location. Grill adds hive lib in the classpath.
#export HIVE_HOME=

# any additional java opts you want to set. This will apply to both client and server operations
#export GRILL_OPTS=

# any additional java opts that you want to set for client only
#export GRILL_CLIENT_OPTS=

# java heap size we want to set for the client. Default is 1024MB
#export GRILL_CLIENT_HEAP=

# any additional opts you want to set for grill server.
export GRILL_SERVER_OPTS="-XX:PermSize=256m -XX:MaxPermSize=256m"

# java heap size we want to set for the grill server. Default is 1024MB
#export GRILL_SERVER_HEAP=

# What is is considered as grill home dir. Default is the base location of the installed software
#export GRILL_HOME_DIR=

# Where log files are stored. Default is logs directory under the base install location
#export GRILL_LOG_DIR=

# Where pid files are stored. Default is logs directory under the base install location
#export GRILL_PID_DIR=

# Where do you want to expand the war file. By Default it is in /webapp dir under the base install dir.
#export GRILL_EXPANDED_WEBAPP_DIR=

#Classpath for grill server or client
#export GRILLCPPATH=
