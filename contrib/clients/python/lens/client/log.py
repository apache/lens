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
from .auth import SpnegoAuth
import requests


class LensLogClient(object):
    def __init__(self, base_url, conf):
        self.base_url = base_url + "logs/"
        self.keytab = conf.get('lens.client.authentication.kerberos.keytab')
        self.principal = conf.get('lens.client.authentication.kerberos.principal')
        self.ignoreCert = conf.get('lens.client.ssl.ignore.server.cert')

    def __getitem__(self, item):
        if self.ignoreCert == 'true':
            return requests.get(self.base_url + str(item), auth=SpnegoAuth(self.keytab, self.principal), verify=False).text
        else:
            return requests.get(self.base_url + str(item), auth=SpnegoAuth(self.keytab, self.principal)).text
