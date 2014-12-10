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

Lens Docker files usable for testing and development purposes.

1. Build the image by running build script: Use tools/scripts/build-docker.sh

2. Run the image in the container: Use tools/scripts/run-docker.sh

This will start the docker container and the container will have its Lens directories
mounted to your actual checked out code, allowing you to modify and recompile
your Lens source and have them immediately usable in the docker images
(without rebuilding the image).
