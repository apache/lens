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
import xml.etree.ElementTree as ET

def to_camel_case(snake_str):
    """
    Converts snake_case to camelCase.
    :param snake_str: a string in snake case
    :return: equivalent string in camel case

    >>> to_camel_case('a_b')
    'aB'
    >>> to_camel_case('some_important_string')
    'someImportantString'
    """

    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + "".join(x.title() for x in components[1:])


def conf_to_xml(conf):
    """
    Converts LensConf given as dictionary to xml string
    :param conf: a Dictionary
    :return: LensConf xml string representation
    >>> conf_to_xml(None)
    '<conf></conf>'
    >>> conf_to_xml({})
    '<conf></conf>'
    >>> conf_to_xml({'a':'b'})
    '<conf><properties><entry><key>a</key><value>b</value></entry></properties></conf>'
    """
    if conf and len(conf) != 0:
        return "<conf><properties>" + \
               "".join("<entry><key>%s</key><value>%s</value></entry>" % (k, v) for k, v in conf.items()) + \
               "</properties></conf>"
    return "<conf></conf>"

def xml_file_to_conf(path):
    """
    Reads file present at path as hadoop configuration file and converts that to a dictionary.
    :param path: Path of xml
    :return: configuration as dictionary

    """
    return {property.find('name').text: property.find('value').text for property in ET.parse(path).getroot()}
