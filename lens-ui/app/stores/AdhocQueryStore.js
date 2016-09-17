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

import assign from 'object-assign';
import { EventEmitter } from 'events';

import AppDispatcher from '../dispatcher/AppDispatcher';
import AdhocQueryConstants from '../constants/AdhocQueryConstants';
import Config from 'config.json';

var CHANGE_EVENT = 'change';
var adhocDetails = {

  queryHandle: null,
  queries: {},
  queryResults: {}, // map with handle being the key
  dbName: Config.dbName
};

function receiveQueryHandle(payload) {
  if (typeof payload.queryHandle === 'string') {
    adhocDetails.queryHandle = payload.queryHandle;
    return;
  }
  let id = payload && payload.queryHandle && payload.queryHandle.lensAPIResult &&
    payload.queryHandle.lensAPIResult.data &&
    payload.queryHandle.lensAPIResult.data.handleId;
  adhocDetails.queryHandle = id;
}

function receiveQueries(payload) {
  adhocDetails.queries = adhocDetails.queries || {};
  payload.queries.forEach((query) => {
    adhocDetails.queries[query.lensQuery.queryHandle.handleId] = query.lensQuery;
  });
}
function receiveQueryHandles(payload) {
  adhocDetails.handles = payload.handles.map(handle=>handle.queryHandle.handleId);
}

function receiveQuery(payload) {
  let query = payload.query;
  adhocDetails.queries[query.queryHandle.handleId] = query;
}

function receiveQueryResult(payload) {
  let queryResult = {};
  queryResult.type = payload && payload.type;

  if (queryResult.type === 'INMEMORY') {
    let resultRows = payload.queryResult && payload.queryResult.rows &&
      payload.queryResult.rows || [];
    let columns = payload.columns && payload.columns.columns;

    adhocDetails.queryResults[payload.handle] = {};
    adhocDetails.queryResults[payload.handle].results = resultRows;
    adhocDetails.queryResults[payload.handle].columns = columns;
  } else {
    // persistent
    adhocDetails.queryResults[payload.handle] = {};
    adhocDetails.queryResults[payload.handle].downloadURL = payload.downloadURL;
  }
}

let AdhocQueryStore = assign({}, EventEmitter.prototype, {
  getQueries () {
    return adhocDetails.queries;
  },
  getQueryHandles () {
    return adhocDetails.handles;
  },
  getQueryDetails (handle) {
    return adhocDetails.queries[handle];
  },
  getQueryResult (handle) {
    return adhocDetails.queryResults[handle];
  },

  // always returns the last-run-query's handle
  getQueryHandle () {
    let handle = adhocDetails.queryHandle;
    adhocDetails.queryHandle = null;
    return handle;
  },

  emitChange (hash) {
    this.emit(CHANGE_EVENT, hash);
  },

  addChangeListener (callback) {
    this.on(CHANGE_EVENT, callback);
  },

  removeChangeListener (callback) {
    this.removeListener(CHANGE_EVENT, callback);
  }
});

AppDispatcher.register((action) => {
  switch (action.actionType) {

    case AdhocQueryConstants.RECEIVE_QUERY_HANDLE:
      receiveQueryHandle(action.payload);
      AdhocQueryStore.emitChange();
      break;

    case AdhocQueryConstants.RECEIVE_QUERIES:
      receiveQueries(action.payload);
      AdhocQueryStore.emitChange();
      break;

    case AdhocQueryConstants.RECEIVE_QUERY_HANDLES:
      receiveQueryHandles(action.payload);
      AdhocQueryStore.emitChange();
      break;

    case AdhocQueryConstants.RECEIVE_QUERY_RESULT:
      receiveQueryResult(action.payload);
      AdhocQueryStore.emitChange();
      break;

    case AdhocQueryConstants.RECEIVE_QUERY:
      receiveQuery(action.payload);
      AdhocQueryStore.emitChange();
      break;

    case AdhocQueryConstants.RECEIVE_QUERY_HANDLE_FAILED:
    case AdhocQueryConstants.RECEIVE_QUERY_HANDLES_FAILED:
      AdhocQueryStore.emitChange(action.payload);
      break;
  }
});

export default AdhocQueryStore;
