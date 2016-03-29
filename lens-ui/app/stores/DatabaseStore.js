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

function receiveDatabases (payload) {
  databases = [];

  databases = payload.databases.stringList &&
    payload.databases.stringList.elements &&
    payload.databases.stringList.elements.slice();
}

let CHANGE_EVENT = 'change';
var databases = [];

let DatabaseStore = assign({}, EventEmitter.prototype, {
  getDatabases () {
    return databases;
  },

  emitChange () {
    this.emit(CHANGE_EVENT);
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
    case AdhocQueryConstants.RECEIVE_DATABASES:
      receiveDatabases(action.payload);
      DatabaseStore.emitChange();
      break;
  }
});

export default DatabaseStore;
