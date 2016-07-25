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
import codecs
import time
import zipfile

import requests
from six import string_types, BytesIO, StringIO, PY2, PY3
from .models import WrappedJson
from .utils import conf_to_xml
import csv

long_type = int

if PY3:
    from collections.abc import Iterable as Iterable
elif PY2:
    from collections import Iterable as Iterable
    long_type = long


class LensQuery(WrappedJson):
    def __init__(self, client, *args, **kwargs):
        super(LensQuery, self).__init__(*args, **kwargs)
        self.client = client

    @property
    def finished(self):
        return self.status.status in ('SUCCESSFUL', 'FAILED', 'CANCELED', 'CLOSED')

    def get_result(self, *args, **kwargs):
        return self.client.get_result(self, *args, **kwargs)

    result = property(get_result)


type_mappings = {'BOOLEAN': bool,
                 'TINYINT': int,
                 'SMALLINT': int,
                 'INT': int,
                 'BIGINT': long_type,
                 'FLOAT': float,
                 'DOUBLE': float,
                 'TIMESTAMP': long_type,
                 'BINARY': bin,
                 'ARRAY': list,
                 'MAP': dict,
                 # 'STRUCT,': str,
                 # 'UNIONTYPE,': float,
                 # 3'USER_DEFINED,': float,
                 'DECIMAL,': float,
                 # 'NULL,': float,
                 # 'DATE,': float,
                 # 'VARCHAR,': float,
                 # 'CHAR': float
                 }
default_mapping = lambda x: x

class LensQueryResult(Iterable):
    def __init__(self, custom_mappings=None):
        if custom_mappings is None:
            custom_mappings = {}
        self.custom_mappings = custom_mappings

    def _mapping(self, type_name):
        if type_name in self.custom_mappings:
            return self.custom_mappings[type_name]
        if type_name in type_mappings:
            return type_mappings[type_name]
        return default_mapping


class LensInMemoryResult(LensQueryResult):
    def __init__(self, resp, custom_mappings=None):
        super(LensInMemoryResult, self).__init__(custom_mappings)
        self.rows = resp.in_memory_query_result.rows

    def __iter__(self):
        for row in self.rows:
            yield list(self._mapping(value.type)(value.value) if value else None for value in row['values'])

class LensPersistentResult(LensQueryResult):
    def __init__(self, header, response, encoding=None, is_header_present=True, delimiter=",",
                 custom_mappings=None):
        super(LensPersistentResult, self).__init__(custom_mappings)
        self.response = response
        self.is_zipped = 'zip' in self.response.headers['content-disposition']
        self.delimiter = str(delimiter)
        self.is_header_present = is_header_present
        self.encoding = encoding
        self.header = header

    def _parse_line(self, line):
        return list(self._mapping(self.header.columns[index].type)(line[index]) for index in range(len(line)))

    def __iter__(self):
        if self.is_zipped:
            byte_stream = BytesIO(self.response.content)
            with zipfile.ZipFile(byte_stream) as self.zipfile:
                for name in self.zipfile.namelist():
                    with self.zipfile.open(name) as single_file:
                        if name[-3:] == 'csv':
                            reader = csv.reader(single_file, delimiter=self.delimiter)
                        else:
                            reader = single_file
                        reader_iterator = iter(reader)
                        if self.is_header_present:
                            next(reader_iterator)
                        for line in reader_iterator:
                            yield self._parse_line(line)
            byte_stream.close()
        else:
            stream = codecs.iterdecode(self.response.iter_lines(),
                                       self.response.encoding or self.response.apparent_encoding)
            reader = csv.reader(stream, delimiter=self.delimiter)
            reader_iterator = iter(reader)
            if self.is_header_present:
                next(reader_iterator)
            for line in reader_iterator:
                yield self._parse_line(line)
            stream.close()


class LensQueryClient(object):
    def __init__(self, base_url, session):
        self._session = session
        self.base_url = base_url + "queryapi/"
        self.launched_queries = []
        self.finished_queries = {}
        self.query_confs = {}
        self.is_header_present_in_result = self._session['lens.query.output.write.header'].lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup']

    def __call__(self, **filters):
        filters['sessionid'] = self._session._sessionid
        resp = requests.get(self.base_url + "queries/", params=filters, headers={'accept': 'application/json'})
        return self.sanitize_response(resp)

    def __getitem__(self, item):
        if isinstance(item, string_types):
            if item in self.finished_queries:
                return self.finished_queries[item]
            resp = requests.get(self.base_url + "queries/" + item, params={'sessionid': self._session._sessionid},
                                headers={'accept': 'application/json'})
            resp.raise_for_status()
            query = LensQuery(self, resp.json(object_hook=WrappedJson))
            if query.finished:
                query.client = self
                self.finished_queries[item] = query
            return query
        elif isinstance(item, LensQuery):
            return self[item.query_handle]
        elif isinstance(item, WrappedJson):
            if item._is_wrapper:
                return self[item._wrapped_value]
            if item.query_handle:
                return self[item.query_handle]
        raise Exception("Can't get query: " + str(item))

    def submit(self, query, operation=None, query_name=None, timeout=None, conf=None, wait=False, fetch_result=False,
               *args, **kwargs):
        payload = [('sessionid', self._session._sessionid), ('query', query)]
        if query_name:
            payload.append(('queryName', query_name))
        if timeout:
            payload.append(('timeoutmillis', str(int(timeout) * 1000)))
        if not operation:
            operation = "execute_with_timeout" if timeout else "execute"
        payload.append(('operation', operation))
        payload.append(('conf', conf_to_xml(conf)))
        resp = requests.post(self.base_url + "queries/", files=payload, headers={'accept': 'application/json'})
        query = self.sanitize_response(resp)
        if conf:
            self.query_confs[str(query)] = conf
        if fetch_result:
            # get result and return
            return self.get_result(query, *args, **kwargs)  # query is handle here
        elif wait:
            # fetch details and return
            return self.wait_till_finish(query, *args, **kwargs)
        # just return handle. This would be the async case. Or execute with timeout, without wait
        return query

    def wait_till_finish(self, handle_or_query, poll_interval=5, *args, **kwargs):
        while not self[handle_or_query].finished:
            time.sleep(poll_interval)
        return self[handle_or_query]

    def get_result(self, handle_or_query, *args, **kwargs):
        query = self.wait_till_finish(handle_or_query, *args, **kwargs)
        handle = str(query.query_handle)
        if query.status.status == 'SUCCESSFUL' and query.status.is_result_set_available:
            resp = requests.get(self.base_url + "queries/" + handle + "/resultsetmetadata",
                                params={'sessionid': self._session._sessionid}, headers={'accept': 'application/json'})
            metadata = self.sanitize_response(resp)
            # Try getting the result through http result
            resp = requests.get(self.base_url + "queries/" + handle + "/httpresultset",
                                params={'sessionid': self._session._sessionid}, stream=True)
            if resp.ok:
                is_header_present = self.is_header_present_in_result
                if handle in self.query_confs and 'lens.query.output.write.header' in self.query_confs[handle]:
                    is_header_present = bool(self.query_confs[handle]['lens.query.output.write.header'])
                return LensPersistentResult(metadata, resp, is_header_present=is_header_present, *args, **kwargs)
            else:
                response = requests.get(self.base_url + "queries/" + handle + "/resultset",
                                    params={'sessionid': self._session._sessionid}, headers={'accept': 'application/json'})
                resp = self.sanitize_response(response)
                # If it has in memory result, return inmemory result iterator
                if resp._is_wrapper and resp._wrapped_key == u'inMemoryQueryResult':
                    return LensInMemoryResult(resp)
                # Else, return whatever you got
                return resp

        else:
            raise Exception("Result set not available")

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
