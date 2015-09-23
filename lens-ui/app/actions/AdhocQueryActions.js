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
import AdhocQueryAdapter from '../adapters/AdhocQueryAdapter';

let AdhocQueryActions = {
  getDatabases (secretToken) {
    AdhocQueryAdapter.getDatabases(secretToken)
      .then(function (databases) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_DATABASES,
          payload: { databases: databases }
        });
      }, function (error) {

        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_DATABASES_FAILED,
          payload: {
            responseCode: error.status,
            responseMessage: error.statusText
          }
        });
      });
  },

  getCubes (secretToken) {
    AdhocQueryAdapter.getCubes(secretToken)
      .then(function (cubes) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_CUBES,
          payload: { cubes: cubes }
        });
      }, function (error) {

        // propagating the error message, couldn't fetch cubes
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_CUBES_FAILED,
          payload: {
            responseCode: error.status,
            responseMessage: error.statusText
          }
        });
      });
  },

  executeQuery (secretToken, query, queryName) {
    AdhocQueryAdapter.executeQuery(secretToken, query, queryName)
      .then(function (queryHandle) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_HANDLE,
          payload: { queryHandle: queryHandle }
        });
      }, function (error) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_HANDLE_FAILED,
          payload: {
            responseCode: error.status,
            responseMessage: error.statusText
          }
        });
      });
  },

  getCubeDetails (secretToken, cubeName) {
    AdhocQueryAdapter.getCubeDetails(secretToken, cubeName)
      .then(function (cubeDetails) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_CUBE_DETAILS,
          payload: { cubeDetails: cubeDetails }
        });
      }, function (error) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_CUBE_DETAILS_FAILED,
          payload: {
            responseCode: error.status,
            responseMessage: error.statusText
          }
        });
      });
  },

  getQueries (secretToken, email, options) {
    AdhocQueryAdapter.getQueries(secretToken, email, options)
      .then(function (queries) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERIES,
          payload: { queries: queries }
        });
      }, function (error) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERIES_FAILED,
          payload: {
            responseCode: error.status,
            responseMessage: error.statusText
          }
        });
      });
  },

  getQuery (secretToken, handle) {
    AdhocQueryAdapter.getQuery(secretToken, handle)
      .then(function (query) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY,
          payload: { query: query }
        });
      }, function (error) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_FAILED,
          payload: {
            responseCode: error.status,
            responseMessage: error.statusText
          }
        });
      });
  },

  getQueryResult (secretToken, handle, queryMode) {
    AdhocQueryAdapter.getQueryResult(secretToken, handle, queryMode)
      .then(function (result) {
        let payload;
        if (Object.prototype.toString.call(result).match('String')) {

          // persistent
          payload = { downloadURL: result, type: 'PERSISTENT', handle: handle };
        } else if (Object.prototype.toString.call(result).match('Array')) {

          // in-memory gives array
          payload = {
            queryResult: result[0],
            columns: result[1],
            handle: handle,
            type: 'INMEMORY'
          };
        }
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_RESULT,
          payload: payload
        });
      }, function (error) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_RESULT_FAILED,
          payload: {
            responseCode: error.status,
            responseMessage: error.statusText
          }
        });
      });
  },

  getTables (secretToken, database) {
    AdhocQueryAdapter.getTables(secretToken, database)
      .then(function (tables) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_TABLES,
          payload: { tables: tables, database: database }
        });
      }, function (error) {

        // propagating the error message, couldn't fetch cubes
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_TABLES_FAILED,
          payload: {
            responseCode: error.status,
            responseMessage: error.statusText
          }
        });
      });
  },

  getTableDetails (secretToken, tableName, database) {
    AdhocQueryAdapter.getTableDetails(secretToken, tableName, database)
      .then(function (tableDetails) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_TABLE_DETAILS,
          payload: { tableDetails: tableDetails, database: database }
        });
      }, function (error) {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_TABLE_DETAILS_FAILED,
          payload: {
            responseCode: error.status,
            responseMessage: error.statusText
          }
        });
      });
  },

  cancelQuery (secretToken, handle) {
    AdhocQueryAdapter.cancelQuery(secretToken, handle);
    // TODO finish this up
  }
};

export default AdhocQueryActions;
