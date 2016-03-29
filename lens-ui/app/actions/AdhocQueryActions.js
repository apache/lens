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
import ErrorParser from '../utils/ErrorParser';
import _ from 'lodash';

function _executeQuery (secretToken, query, queryName) {
  AdhocQueryAdapter.executeQuery(secretToken, query, queryName)
    .then(queryHandle => {
      AppDispatcher.dispatch({
        actionType: AdhocQueryConstants.RECEIVE_QUERY_HANDLE,
        payload: { queryHandle: queryHandle.lensAPIResult &&
          queryHandle.lensAPIResult.data &&
          queryHandle.lensAPIResult.data.handleId }
      });
    }, (error) => {
      // error details contain array of objects {code, message}
      var errorDetails = ErrorParser.getMessage(error);
      AppDispatcher.dispatch({
        actionType: AdhocQueryConstants.RECEIVE_QUERY_HANDLE_FAILED,
        payload: {
          type: 'Error',
          texts: errorDetails
        }
      });
    });
}

function _saveQuery (secretToken, user, query, options) {
  AdhocQueryAdapter.saveQuery(secretToken, user, query, options)
    .then(response => {
      AppDispatcher.dispatch({
        actionType: AdhocQueryConstants.SAVE_QUERY_SUCCESS,
        payload: {
          type: 'Success',
          text: 'Query was successfully saved!',
          id: response.resourceModifiedResponse && response.resourceModifiedResponse.id
        }
      });
    }, error => {
      error = error.lensAPIResult.error;
      AppDispatcher.dispatch({
        actionType: AdhocQueryConstants.SAVE_QUERY_FAILED,
        payload: {type: 'Error', text: error.code + ': ' + error.message}
      });
    }).catch(e => { console.error(e); });
}

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

  getSavedQueries (secretToken, user, options) {
    AdhocQueryAdapter.getSavedQueries(secretToken, user, options)
      .then(savedQueries => {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_SAVED_QUERIES,
          payload: savedQueries
        });
      });
  },

  getSavedQueryById (secretToken, id) {
    AdhocQueryAdapter.getSavedQueryById(secretToken, id)
      .then(savedQuery => {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_SAVED_QUERY,
          payload: savedQuery
        });
      });
  },

  updateSavedQuery (secretToken, user, query, options, id) {
    AdhocQueryAdapter.getParams(secretToken, query).then(response => {
      let serverParams = response.parameterParserResponse.parameters
        .map(item => item.name)
        .sort();
      let clientParams = options && options.parameters && options.parameters
        .map(item => item.name)
        .sort();
      if (!clientParams) clientParams = [];
      if (_.isEqual(serverParams, clientParams)) {
        AdhocQueryAdapter.updateSavedQuery(secretToken, user, query, options, id)
          .then(response => {
            AppDispatcher.dispatch({
              actionType: AdhocQueryConstants.SAVE_QUERY_SUCCESS,
              payload: {
                type: 'Success',
                text: 'Query was successfully updated!',
                id: response.resourceModifiedResponse && response.resourceModifiedResponse.id
              }
            });
          }, error => {
            error = error.lensAPIResult.error;
            AppDispatcher.dispatch({
              actionType: AdhocQueryConstants.SAVE_QUERY_FAILED,
              payload: {type: 'Error', text: error.code + ': ' + error.message}
            });
          }).catch(e => { console.error(e); });
      } else {
        // get parameters' meta
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_PARAMS_META,
          payload: response.parameterParserResponse.parameters
        });
      }
    });
  },

  saveQuery (secretToken, user, query, options) {
    AdhocQueryAdapter.getParams(secretToken, query).then(response => {
      let serverParams = response.parameterParserResponse.parameters
        .map(item => item.name)
        .sort();
      let clientParams = options && options.parameters && options.parameters
        .map(item => item.name)
        .sort();
      if (!clientParams) clientParams = [];
      if (_.isEqual(serverParams, clientParams)) {
        _saveQuery(secretToken, user, query, options);
      } else {
        // get parameters' meta
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_PARAMS_META,
          payload: response.parameterParserResponse.parameters
        });
      }
    }, error => {
      AppDispatcher.dispatch({
        actionType: AdhocQueryConstants.RECEIVE_QUERY_HANDLE_FAILED,
        payload: {
          type: 'Error',
          text: 'Please enable Saved Queries feature in the LENS Server to proceed.'
        }
      });
    });
  },

  // first calls parameters API and sees if the query has any params,
  // as we can't run a query with params, it needs to be saved first.
  runQuery (secretToken, query, queryName) {
    AdhocQueryAdapter.getParams(secretToken, query).then(response => {
      if (!response.parameterParserResponse.parameters) {
        _executeQuery(secretToken, query, queryName);
      } else {
        // ask user to save the query maybe?
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_HANDLE_FAILED,
          payload: {
            type: 'Error',
            text: 'You can\'t run a query with parameters, save it first.'
          }
        });
      }
    }, error => {
      AppDispatcher.dispatch({
        actionType: AdhocQueryConstants.RECEIVE_QUERY_HANDLE_FAILED,
        payload: {
          type: 'Error',
          text: 'Please enable Saved Queries feature in the LENS Server to proceed.'
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
          payload: { query: query.lensQuery }
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
          payload = {
            queryResult: result[0].inMemoryQueryResult,
            columns: result[1].queryResultSetMetadata,
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
  },

  runSavedQuery (secretToken, id, params) {
    AdhocQueryAdapter.runSavedQuery(secretToken, id, params)
      .then(handle => {
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_HANDLE,
          payload: { queryHandle: handle }
        });
      }, (error) => {
        error = error.lensAPIResult.error;
        AppDispatcher.dispatch({
          actionType: AdhocQueryConstants.RECEIVE_QUERY_HANDLE_FAILED,
          payload: {
            type: 'Error',
            text: error.code + ': ' +
              error.message
          }
        });
      });
  }
};

export default AdhocQueryActions;
