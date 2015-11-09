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

import Promise from 'bluebird';

import BaseAdapter from './BaseAdapter';
import Config from 'config.json';

let baseUrl = Config.baseURL;
let urls = {
  getDatabases: 'metastore/databases',
  getCubes: 'metastore/cubes',
  query: 'queryapi/queries', // POST on this to execute, GET to fetch all
  getTables: 'metastore/nativetables',
  getSavedQueries: 'queryapi/savedqueries',
  parameters: 'queryapi/savedqueries/parameters',
  saveQuery: 'queryapi/savedqueries', // POST to save, PUT to update, {id} for GET
  runSavedQuery: 'queryapi/savedqueries'
};

let AdhocQueryAdapter = {
  getDatabases (secretToken) {
    let url = baseUrl + urls.getDatabases;
    return BaseAdapter.get(url, { sessionid: secretToken });
  },

  getCubes (secretToken) {
    let url = baseUrl + urls.getCubes;
    return BaseAdapter.get(url, { sessionid: secretToken });
  },

  getCubeDetails (secretToken, cubeName) {
    let url = baseUrl + urls.getCubes + '/' + cubeName;
    return BaseAdapter.get(url, { sessionid: secretToken });
  },

  executeQuery (secretToken, query, queryName) {
    let url = baseUrl + urls.query;

    let formData = new FormData();
    formData.append('sessionid', secretToken);
    formData.append('query', query);
    formData.append('operation', 'execute');

    if (queryName) formData.append('queryName', queryName);

    return BaseAdapter.post(url, formData, {
      contentType: 'multipart/form-data'
    });
  },

  saveQuery (secretToken, user, query, options) {
    let url = baseUrl + urls.saveQuery;
    let queryToBeSaved = {
      owner: user,
      name: options.name || '',
      query: query,
      description: options.description || '',
      parameters: options.parameters || []
    };

    return BaseAdapter.post(url, queryToBeSaved);
  },

  updateSavedQuery (secretToken, user, query, options, id) {
    let url = baseUrl + urls.saveQuery + '/' + id;
    let queryToBeSaved = {
      owner: user,
      name: options.name || '',
      query: query,
      description: options.description || '',
      parameters: options.parameters || []
    };

    return BaseAdapter.put(url, queryToBeSaved);
  },

  getQuery (secretToken, handle) {
    let url = baseUrl + urls.query + '/' + handle;
    return BaseAdapter.get(url, {sessionid: secretToken});
  },

  getQueries (secretToken, email, options) {
    let url = baseUrl + urls.query;

    let queryOptions = {};
    queryOptions.sessionid = secretToken;
    queryOptions.user = email;

    if (options && options.state) {
      queryOptions.state = options.state.toUpperCase();
    }

    return BaseAdapter.get(url, queryOptions)
      .then(function (queryHandles) {
        // FIXME limiting to 10 for now
        // let handles = queryHandles.slice(0, 10);
        return Promise.all(queryHandles.map((handle) => {
          return BaseAdapter.get(url + '/' + handle.handleId, {
            sessionid: secretToken,
            queryHandle: handle.handleId
          });
        }));
      });
  },

  getQueryResult (secretToken, handle, queryMode) {
    // on page refresh, the store won't have queryMode so fetch query
    // this is needed as we won't know in which mode the query was fired
    if (!queryMode) {
      this.getQuery(secretToken, handle).then((query) => {
        queryMode = query.isPersistent;
        queryMode = queryMode ? 'PERSISTENT' : 'INMEMORY';
        return this._inMemoryOrPersistent(secretToken, handle, queryMode);
      });
    } else {
      return this._inMemoryOrPersistent(secretToken, handle, queryMode);
    }
  },

  _inMemoryOrPersistent (secretToken, handle, queryMode) {
    return queryMode === 'PERSISTENT' ?
      this.getDownloadURL(secretToken, handle) :
      this.getInMemoryResults(secretToken, handle);
  },

  getTables (secretToken, database) {
    let url = baseUrl + urls.getTables;
    return BaseAdapter.get(url, {
      sessionid: secretToken,
      dbName: database
    });
  },

  getTableDetails (secretToken, tableName, database) {
    let url = baseUrl + urls.getTables + '/' + database + '.' + tableName;
    return BaseAdapter.get(url, { sessionid: secretToken });
  },

  cancelQuery (secretToken, handle) {
    let url = baseUrl + urls.query + '/' + handle + '?sessionid=' + secretToken;
    return BaseAdapter.delete(url);
  },

  getDownloadURL (secretToken, handle) {
    let downloadURL = baseUrl + urls.query + '/' + handle +
      '/httpresultset?sessionid=' + secretToken;

    return Promise.resolve(downloadURL);
  },

  getSavedQueryById (secretToken, id) {
    let url = baseUrl + urls.saveQuery + '/' + id;
    return BaseAdapter.get(url, {sessionid: secretToken});
  },

  getInMemoryResults (secretToken, handle) {
    let resultUrl = baseUrl + urls.query + '/' + handle + '/resultset';
    let results = BaseAdapter.get(resultUrl, {
      sessionid: secretToken
    });

    let metaUrl = baseUrl + urls.query + '/' + handle + '/resultsetmetadata';
    let meta = BaseAdapter.get(metaUrl, {
      sessionid: secretToken
    });

    return Promise.all([results, meta]);
  },

  getSavedQueries (secretToken, user, options = {}) {
    let url = baseUrl + urls.getSavedQueries;
    return BaseAdapter.get(url, {
      user: user,
      sessionid: secretToken,
      start: options.offset || 0,
      count: options.pageSize || 10
    });
  },

  getParams (secretToken, query) {
    let url = baseUrl + urls.parameters;

    let formData = new FormData();
    formData.append('query', query);

    return BaseAdapter.post(url, formData);
  },

  runSavedQuery (secretToken, id, params) {
    let queryParamString = BaseAdapter.jsonToQueryParams(params);
    let url = baseUrl + urls.runSavedQuery + '/' + id + queryParamString;

    let formData = new FormData();
    formData.append('sessionid', secretToken);

    return BaseAdapter.post(url, formData, {
      contentType: 'multipart/form-data'
    });
  }
};

export default AdhocQueryAdapter;
