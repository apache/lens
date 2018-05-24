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
import os

from six import string_types
from .log import LensLogClient
from .session import LensSessionClient
from .query import LensQueryClient
from .utils import xml_file_to_conf
import logging
logger = logging.getLogger(__name__)

class LensClient(object):
    def __init__(self, base_url=None, username="", password="", database=None, conf=None, logging_level=logging.INFO):
        logging.basicConfig(level=logging_level)
        if conf and isinstance(conf, string_types) and os.path.exists(conf):
            if os.path.isdir(conf):
                conf = os.path.join(conf, 'lens-client-site.xml')
            if os.path.exists(conf):
                conf = xml_file_to_conf(conf)
        if not conf:
            conf = {}
        self.base_url = base_url or conf.get('lens.server.base.url', "http://0.0.0.0:9999/lensapi")
        if self.base_url[-1] != '/':
            self.base_url += "/"
        logger.debug("Using conf %s", conf)
        username = username or conf.get('lens.client.user.name', "anonymous")
        database = database or conf.get('lens.client.dbname')
        self.session = LensSessionClient(self.base_url, username, password, database, conf)
        self.queries = LensQueryClient(self.base_url, self.session, conf)
        self.logs = LensLogClient(self.base_url, conf)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()