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

function receiveTables (payload) {
  let database = payload.database;

  if (!tables[database]) {
    tables[database] = {};
    tableCompleteness[database] = true;
  }

  payload.tables.stringList && payload.tables.stringList.elements &&
    payload.tables.stringList.elements.forEach(table => {
      if (!tables[database][table]) {
        tables[database][table] = { name: table, isLoaded: false };
      }
    });
}

function receiveTableDetails (payload) {
  if (payload.tableDetails && payload.tableDetails.x_native_table) {
    // all table details are wrapped in `x_native_table` key, over-write
    payload.tableDetails = payload.tableDetails.x_native_table;
    let database = payload.database;
    let name = payload.tableDetails.name;
    let table = assign({}, payload.tableDetails);
    let columns = table.columns && table.columns.column || [];
    table.columns = columns;

    // check if tables contains the database and table entry,
    // it won't be present when user directly arrived on this link.
    if (!tables[database]) {
      tables[database] = {};
    }

    if (!tables[database][name]) tables[database][name] = {};

    tables[database][name] = table;
    tables[database][name].isLoaded = true;
  }
}

let CHANGE_EVENT = 'change';
var tables = {};
var tableCompleteness = {};

let TableStore = assign({}, EventEmitter.prototype, {
  getTables (database) {
    return tables[database];
  },

  areTablesCompletelyFetched (database) {
    return tableCompleteness[database];
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
    case AdhocQueryConstants.RECEIVE_TABLES:
      receiveTables(action.payload);
      TableStore.emitChange();
      break;

    case AdhocQueryConstants.RECEIVE_TABLE_DETAILS:
      receiveTableDetails(action.payload);
      TableStore.emitChange();
      break;

    case AdhocQueryConstants.RECEIVE_TABLES_FAILED:
      TableStore.emitChange();
      break;
  }
});

export default TableStore;
