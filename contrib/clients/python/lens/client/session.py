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

import requests

from .auth import SpnegoAuth
from .models import WrappedJson
from .utils import conf_to_xml


class LensSessionClient(object):
    def __init__(self, base_url, username, password, database, conf):
        self.base_url = base_url + "session/"
        self.keytab = conf.get('lens.client.authentication.kerberos.keytab')
        self.principal = conf.get('lens.client.authentication.kerberos.principal')
        self.ignoreCert = conf.get('lens.client.ssl.ignore.server.cert')
        self.open(username, password, database, conf)

    def __getitem__(self, key):
        if self.ignoreCert == 'true':
            resp = requests.get(self.base_url + "params",
                            params={'sessionid': self._sessionid, 'key': key},
                            headers={'accept': 'application/json'},
                            auth=SpnegoAuth(self.keytab, self.principal), verify=False)
        else:
            resp = requests.get(self.base_url + "params",
                                        params={'sessionid': self._sessionid, 'key': key},
                                        headers={'accept': 'application/json'},
                                        auth=SpnegoAuth(self.keytab, self.principal))
        if resp.ok:
            params = resp.json(object_hook=WrappedJson)
            text = params.elements[0]
            if key in text:
                text = text[len(key)+1:]
            return text

    def open(self, username, password, database, conf):
        payload = [('username', username), ('password', password), ('sessionconf', conf_to_xml(conf))]
        if database:
            payload.append(('database', database))

        if self.ignoreCert == 'true':
            r = requests.post(self.base_url, files=payload, headers={'accept': 'application/xml'},
                          auth=SpnegoAuth(self.keytab, self.principal), verify=False)
        else:
            r = requests.post(self.base_url, files=payload, headers={'accept': 'application/xml'},
                                      auth=SpnegoAuth(self.keytab, self.principal))
        r.raise_for_status()
        self._sessionid = r.text

    def close(self):
        if self.ignoreCert == 'true':
            requests.delete(self.base_url, params={'sessionid': self._sessionid},
                        auth=SpnegoAuth(self.keytab, self.principal), verify=False)
        else:
            requests.delete(self.base_url, params={'sessionid': self._sessionid},
                                    auth=SpnegoAuth(self.keytab, self.principal))



