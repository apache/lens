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

import AppDispatcher from '../dispatcher/AppDispatcher';
import AdhocQueryConstants from '../constants/AdhocQueryConstants';
import assign from 'object-assign';
import { EventEmitter } from 'events';

let savedQueries = {};
let offset = 0;
let totalRecords = 0;
let CHANGE_EVENT = 'change';

function receiveSavedQueries (payload) {
  payload && payload.listResponse && payload.listResponse.resoures.forEach(query => {
    savedQueries[query.id] = query;
  });

  totalRecords = payload.listResponse && payload.listResponse.totalCount;
}

function receiveSavedQuery (payload) {
  if (!savedQueries[payload.id]) totalRecords++;
  savedQueries[payload.id] = payload.savedQuery;
}

let SavedQueryStore = assign({}, EventEmitter.prototype, {
  getSavedQueries () {
    return savedQueries;
  },

  getTotalRecords () {
    return totalRecords;
  },

  getOffset () {
    return offset;
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
    case AdhocQueryConstants.RECEIVE_SAVED_QUERIES:
      receiveSavedQueries(action.payload);
      SavedQueryStore.emitChange();
      break;

    case AdhocQueryConstants.RECEIVE_QUERY_PARAMS_META:
      SavedQueryStore.emitChange({type: 'params', params: action.payload});
      break;

    case AdhocQueryConstants.SAVE_QUERY_SUCCESS:
      SavedQueryStore.emitChange({
        type: 'success',
        message: action.payload,
        id: action.payload && action.payload.id
      });
      break;

    case AdhocQueryConstants.SAVE_QUERY_FAILED:
      SavedQueryStore.emitChange({type: 'failure', message: action.payload});
      break;

    case AdhocQueryConstants.RECEIVE_SAVED_QUERY:
      receiveSavedQuery(action.payload);
      SavedQueryStore.emitChange();
      break;
  }
});

export default SavedQueryStore;
