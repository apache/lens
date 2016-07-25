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
from __future__ import print_function

import glob
import os
import random
import string
import subprocess
import time
from contextlib import contextmanager

import pytest
from lens.client.models import WrappedJson
from requests.exceptions import HTTPError

from lens.client import LensClient


def check_output(command):
    output = subprocess.check_output(command.split())
    if isinstance(output, bytes):  # For Python 3. Python 2 directly gives string
        output = output.decode("utf-8")
    return output


@contextmanager
def cwd(dir):
    cur_dir = os.getcwd()
    os.chdir(dir)
    yield
    os.chdir(cur_dir)


def time_sorted_ls(path):
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    return list(sorted(os.listdir(path), key=mtime))


def has_error(msg):
    return any(x in msg for x in ('Error', 'error', 'Exception', 'exception'))


def get_error():
    latest_out_file = list(name for name in time_sorted_ls('logs') if 'lensserver.out' in name)[-1]
    print(latest_out_file)
    with open(os.path.join('logs', latest_out_file)) as f:
        return f.read()


def select_query(path):
    with open(path) as f:
        for line in f:
            if 'cube select' in line and 'sample_cube' in line:
                return line


class TestLensClient(object):
    query = "cube select dim1, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')"
    expected_result = [[21, 100], [22, 200], [23, 300], [24, 400], [25, 500], [26, 600], [27, 700], [28, 800]]

    @classmethod
    def setup_class(cls):
        cls.db = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        try:
            root = check_output("git rev-parse --show-toplevel")
        except:
            root = os.getenv("LENS_GIT_PATH")
        print(root, root.strip())
        joined = os.path.join(root.strip(), 'lens-dist', 'target', '*bin', '*bin')
        cls.base_path = glob.glob(joined)[0]
        with cwd(cls.base_path):
            with cwd('server'):
                server_start_output = check_output("bin/lens-ctl restart")
                assert "Started lens server" in server_start_output
                assert os.path.exists('logs/server.pid')
                time.sleep(1)
                while not os.path.exists('logs/lensserver.log'):
                    error = get_error()
                    if has_error(error):
                        # Assert again with complete error
                        assert os.path.exists('logs/lensserver.log'), error
                    time.sleep(1)
                error = get_error()
                if has_error(error):
                    assert False, error

            with cwd('client'):
                cls.candidate_query = select_query('examples/resources/cube-queries.sql')
                with open('check_connection.sql', 'w') as f:
                    f.write('show databases')
                for i in range(100):
                    try:
                        output = check_output('bin/lens-cli.sh --cmdfile check_connection.sql')
                        if not has_error(output):
                            break
                    except:
                        # Ignore error and retry
                        pass
                    time.sleep(1)
                os.remove('check_connection.sql')
                create_output = check_output('bin/run-examples.sh sample-metastore -db ' + cls.db)
                if has_error(create_output):
                    raise Exception("Couldn't create sample metastore: " + create_output)
                populate_output = check_output('bin/run-examples.sh populate-metastore -db ' + cls.db)
                if has_error(populate_output):
                    raise Exception("Couldn't populate sample metastore: " + populate_output)

    @classmethod
    def teardown_class(cls):
        # TODO: drop database
        with cwd(cls.base_path):
            with cwd('client'):
                with open('drop_db.sql', 'w') as f:
                    f.write("drop database {db} --cascade".format(db=cls.db))
                drop_output = check_output('bin/lens-cli.sh --cmdfile drop_db.sql')
                if has_error(drop_output):
                    raise Exception("Couldn't drop db")
                os.remove('drop_db.sql')
            with cwd('server'):
                stop_output = check_output('bin/lens-ctl stop')
                if has_error(stop_output):
                    raise ("Error stopping server: " + stop_output)

    def get_client(self):
        return LensClient(database=self.db, conf=os.path.join(self.base_path, 'client', 'conf'))

    def test_wrong_query(self):
        with self.get_client() as client:
            with pytest.raises(HTTPError) as e:
                client.queries.submit("blah")
            assert e.value.response.status_code == 400;
            assert 'Syntax Error' in e.value.response.json(object_hook=WrappedJson).lens_a_p_i_result.error.message

    def test_submit_query(self):
        with self.get_client() as client:
            handle = client.queries.submit(self.candidate_query)
        with pytest.raises(HTTPError) as e:
            # Either of these can give 410
            client.queries.wait_till_finish(handle)
            client.queries.submit(self.candidate_query)
        assert e.value.response.status_code == 410

    def test_list_query(self):
        with self.get_client() as client:
            handle = client.queries.submit(self.candidate_query, query_name="Candidate Query")
            finished_query = client.queries.wait_till_finish(handle)
            assert client.queries[handle] == finished_query
            queries = client.queries(state='SUCCESSFUL', fromDate=finished_query.submission_time - 1,
                                     toDate=finished_query.submission_time + 1)
            assert handle in queries

    def test_non_persisted_result(self):
        with self.get_client() as client:
            result = client.queries.submit(self.query, fetch_result=True)
            assert str(result)[:30] == 'file:/tmp/lensreports/hdfsout/'

    def test_persisted_result(self):
        with self.get_client() as client:
            result = client.queries.submit(self.query, conf={'lens.query.enable.persistent.resultset': True},
                                           delimiter=u'\x01', fetch_result=True)
            assert list(iter(result)) == self.expected_result

    def test_persistent_result_with_header(self):
        with self.get_client() as client:
            result = client.queries.submit(self.query,
                                           conf={'lens.query.enable.persistent.resultset': True,
                                                 'lens.query.output.write.header': True},
                                           delimiter=u'\x01', fetch_result=True)
            assert list(iter(result)) == self.expected_result

    def test_inmemory_result(self):
        with self.get_client() as client:
            result = client.queries.submit(self.query,
                                           conf={'lens.query.enable.persistent.resultset.indriver': False},
                                           fetch_result=True)
            assert list(iter(result)) == self.expected_result