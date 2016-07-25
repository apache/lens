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
import json

from .utils import to_camel_case


class WrappedJson(dict):
    _is_wrapper = False

    def __init__(self, seq=None, **kwargs):
        super(WrappedJson, self).__init__(seq, **kwargs)
        if len(self) == 1:
            (self._wrapped_key, self._wrapped_value), = self.items()
            self._is_wrapper = True

    def __getitem__(self, item):
        for name in {item, to_camel_case(item)}:
            if name in self:
                return super(WrappedJson, self).__getitem__(name)
            if self._is_wrapper:
                if name in self._wrapped_value:
                    return self._wrapped_value[name]
        raise Exception("Couldn't access " + str(item) + " in " + str(self))

    def __getattr__(self, item):
        try:
            return self.__getitem__(item)
        except:
            pass

    def __str__(self):
        if self._is_wrapper:
            return str(self._wrapped_value)
        return json.dumps(self, indent=2)

    def __repr__(self):
        if self._is_wrapper:
            return str(self._wrapped_value)
        return super(WrappedJson, self).__repr__()

    def __eq__(self, other):
        return super(WrappedJson, self).__eq__(other) or (
        self._is_wrapper and other._is_wrapper and str(self) == str(other))
