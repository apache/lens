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
import kerberos
from requests.auth import AuthBase
import subprocess
import threading
from urlparse import urlparse


class SpnegoAuth(AuthBase):
    def __init__(self, keytab=None, user=None):
        self._thread_local = threading.local()
        self.keytab = keytab
        self.user = user

    def __call__(self, request):
        self.init_per_thread_state()
        request.register_hook('response', self.handle_response)
        self._thread_local.num_401_calls = 1
        return request

    def has_tgt(self):
        # if tgt is available return
        return subprocess.call(['klist', '-s']) == 0

    def acquire_tgt(self):
        # try to kinit
        exit_code = subprocess.call(['kinit', '-k', '-t', self.keytab, self.user])
        if exit_code != 0:
            raise Exception("Couldn't acquire TGT")

    def init_per_thread_state(self):
        # Ensure state is initialized just once per-thread
        if not hasattr(self._thread_local, 'init'):
            self._thread_local.init = True
            self._thread_local.num_401_calls = None

    def handle_response(self, response, **kwargs):
        if response.status_code == 401 and self._thread_local.num_401_calls < 2:
            self._thread_local.num_401_calls += 1
            return self.handle_401(response, **kwargs)

        self._thread_local.num_401_calls += 1
        return response

    def handle_401(self, response, **kwargs):
        s_auth = response.headers.get('www-authenticate', '')
        if "negotiate" in s_auth.lower():
            # try to acquire tgt
            if not self.has_tgt() and self.keytab is not None and self.user is not None:
                self.acquire_tgt()
            host = urlparse(response.url).hostname
            spn = 'HTTP/' + host
            code, krb_context = kerberos.authGSSClientInit(spn)
            kerberos.authGSSClientStep(krb_context, "")
            negotiate_details = kerberos.authGSSClientResponse(krb_context)
            auth_header = "Negotiate " + negotiate_details

            # Consume content and release the original connection
            # to allow our new request to reuse the same one.
            response.content
            response.close()

            response.request.headers['Authorization'] = auth_header
            _resp = response.connection.send(response.request, **kwargs)
            return _resp

        return response

