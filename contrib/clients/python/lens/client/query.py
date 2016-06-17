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
import time
from six import string_types
from .models import WrappedJson, LensQuery
from .utils import conf_to_xml


class LensQueryClient(object):
    def __init__(self, base_url, sessionid):
        self._sessionid = sessionid
        self.base_url = base_url + "queryapi/"
        self.launched_queries = []
        self.finished_queries = {}

    def __call__(self, **filters):
        filters['sessionid'] = self._sessionid
        resp = requests.get(self.base_url + "queries/", params=filters, headers={'accept': 'application/json'})
        return self.sanitize_response(resp)

    def __getitem__(self, item):
        if isinstance(item, string_types):
            if item in self.finished_queries:
                return self.finished_queries[item]
            resp = requests.get(self.base_url + "queries/" + item, params={'sessionid': self._sessionid},
                                           headers={'accept': 'application/json'})
            resp.raise_for_status()
            query = LensQuery(resp.json(object_hook=WrappedJson))
            if query.finished:
                self.finished_queries[item] = query
            return query
        elif isinstance(item, WrappedJson):
            if item._is_wrapper:
                return self[item._wrapped_value]
        raise Exception("Can't get query: " + str(item))

    def submit(self, query, operation="execute", query_name=None, timeout=None, conf=None):
        payload = [('sessionid', self._sessionid), ('query', query), ('operation', operation)]
        if query_name:
            payload.append(('queryName', query_name))
        if timeout:
            payload.append(('timeoutmillis', timeout))
        payload.append(('conf', conf_to_xml(conf)))
        resp = requests.post(self.base_url + "queries/", files=payload, headers={'accept': 'application/json'})
        query = self.sanitize_response(resp)
        self.launched_queries.append(query)
        return query

    def wait_till_finish(self, handle):
        while not self[handle].finished:
            time.sleep(1)
        return self[handle]

    def sanitize_response(self, resp):
        resp.raise_for_status()
        try:
            resp_json = resp.json(object_hook=WrappedJson)
            if 'lensAPIResult' in resp_json:
                resp_json = resp_json.lens_a_p_i_result
                if 'error' in resp_json:
                    raise Exception(resp_json['error'])
                if 'data' in resp_json:
                    data = resp_json.data
                    if len(data) == 2 and 'type' in data:
                        keys = list(data.keys())
                        keys.remove('type')
                        return WrappedJson({data['type']: data[keys[0]]})
                    return data
        except:
            resp_json = resp.json()
        return resp_json


