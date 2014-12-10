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

# The hadoop installation location. 
#export HADOOP_HOME=

# The Hive installation location. Lens adds hive lib in the classpath.
#export HIVE_HOME=

# any additional java opts you want to set. This will apply to both client and server operations
#export LENS_OPTS=

# any additional java opts that you want to set for client only
#export LENS_CLIENT_OPTS=

# java heap size we want to set for the client. Default is 1024MB
#export LENS_CLIENT_HEAP=

# any additional opts you want to set for lens server.
export LENS_SERVER_OPTS="-XX:PermSize=256m -XX:MaxPermSize=256m"

# java heap size we want to set for the lens server. Default is 1024MB
#export LENS_SERVER_HEAP=

# What is is considered as lens home dir. Default is the base location of the installed software
#export LENS_HOME_DIR=

# Where log files are stored. Default is logs directory under the base install location
#export LENS_LOG_DIR=

# Where pid files are stored. Default is logs directory under the base install location
#export LENS_PID_DIR=

# Where do you want to expand the war file. By Default it is in /webapp dir under the base install dir.
#export LENS_EXPANDED_WEBAPP_DIR=

#Classpath for lens server or client
#export LENSCPPATH=
