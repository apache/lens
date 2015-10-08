/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

import KeyMirror from 'keymirror';

const AdhocQueryConstants = KeyMirror({
  RECEIVE_CUBES: null,
  RECEIVE_CUBES_FAILED: null,

  RECEIVE_QUERY_HANDLE: null,
  RECEIVE_QUERY_HANDLE_FAILED: null,

  RECEIVE_CUBE_DETAILS: null,
  RECEIVE_CUBE_DETAILS_FAILED: null,

  RECEIVE_QUERIES: null,
  RECEIVE_QUERIES_FAILED: null,

  RECEIVE_QUERY_RESULT: null,
  RECEIVE_QUERY_RESULT_FAILED: null,

  RECEIVE_TABLES: null,
  RECEIVE_TABLES_FAILED: null,

  RECEIVE_TABLE_DETAILS: null,
  RECEIVE_TABLE_DETAILS_FAILED: null,

  RECEIVE_QUERY: null,
  RECEIVE_QUERY_FAILED: null,

  RECEIVE_DATABASES: null,
  RECEIVE_DATABASES_FAILED: null,

  RECEIVE_QUERY_PARAMS_META: null,

  SAVE_QUERY_SUCCESS: null,
  SAVE_QUERY_FAILED: null,

  RECEIVE_SAVED_QUERY: null
});

export default AdhocQueryConstants;
