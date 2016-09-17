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
  setDatabases: 'metastore/databases/current',
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
    return BaseAdapter.get(url + '?sessionid=' + secretToken);
  },
  setDatabase (secretToken, database) {
    let url = baseUrl + urls.setDatabases;
    return BaseAdapter.put(url + '?sessionid=' + secretToken, database, {
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });
  },

  getCubes (secretToken) {
    let url = baseUrl + urls.getCubes;
    let postURL = "?";
    if (Config.cubes_type) {
      postURL += "type=" + Config.cubes_type + "&"
    }
    postURL += "sessionid=" + secretToken;
    return BaseAdapter.get(url + postURL);
  },

  getCubeDetails (secretToken, cubeName) {
    let url = baseUrl + urls.getCubes + '/' + cubeName;
    return BaseAdapter.get(url + '?sessionid=' + secretToken);
  },

  executeQuery (secretToken, query, queryName) {
    let url = baseUrl + urls.query;

    let formData = new FormData();
    formData.append('sessionid', secretToken);
    formData.append('query', query);
    formData.append('operation', 'EXECUTE');
    formData.append('conf',
      '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><conf></conf>');

    if (queryName) formData.append('queryName', queryName);

    return BaseAdapter.post(url, formData, {
      headers: {
        'Accept': 'application/json'
      }
    });
  },

  saveQuery (secretToken, user, query, options) {
    let queryToBeSaved = {
      savedQuery: {
        name: options.name || '',
        query: query,
        description: options.description || '',
        parameters: options.parameters || []
      }
    };
    let url = baseUrl + urls.saveQuery + '?sessionid=' + secretToken;
    return BaseAdapter.post(url, JSON.stringify(queryToBeSaved), {
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });
  },

  updateSavedQuery (secretToken, user, query, options, id) {
    let url = baseUrl + urls.saveQuery + '/' + id;
    let queryToBeSaved = {
      savedQuery: {
        owner: user,
        name: options.name || '',
        query: query,
        description: options.description || '',
        parameters: options.parameters || []
      }
    };

    return BaseAdapter.put(url, JSON.stringify(queryToBeSaved), {
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });
  },

  getQuery (secretToken, handle) {
    let url = baseUrl + urls.query + '/' + handle;
    return BaseAdapter.get(url + '?sessionid=' + secretToken);
  },

  getQueries (secretToken, email, options) {
    let queryOptions = {};
    queryOptions.sessionid = secretToken;
    queryOptions.user = email;
    var state;
    if (options && options.state) {
      state = options.state.toUpperCase();
    }
    let handlesUrl = baseUrl + urls.query + '?sessionid=' + secretToken + '&user=' +
      email;
    if (state) handlesUrl += '&state=' + state;
    return BaseAdapter.get(handlesUrl)
      .then(function (queryHandles) {
        // FIXME limiting to 10 for now
        // let handles = queryHandles.slice(0, 10);
        return Promise.all(queryHandles.map((q) => {
          let queryUrl = baseUrl + urls.query + '/' + q.queryHandle.handleId +
            '?sessionid=' + secretToken + '&queryHandle=' + q.queryHandle.handleId;

          return BaseAdapter.get(queryUrl);
        }));
      });
  },
  getQueryHandles (secretToken, email, options) {
    let queryOptions = {};
    queryOptions.sessionid = secretToken;
    queryOptions.user = email;
    var state;
    if (options && options.state) {
      state = options.state.toUpperCase();
    }
    let handlesUrl = baseUrl + urls.query + '?sessionid=' + secretToken + '&user=' +
      email;
    if (state) handlesUrl += '&state=' + state;
    if (options.fromDate) handlesUrl += "&fromDate="+options.fromDate;
    if (options.toDate) handlesUrl += "&toDate="+options.toDate;
    return BaseAdapter.get(handlesUrl);
  },
  getQueriesDetails (secretToken, handles) {
    let url = baseUrl + urls.query + '?sessionid=' + secretToken;
    return Promise.all(handles.map((handle) => {
      let queryUrl = baseUrl + urls.query + '/' + handle +
        '?sessionid=' + secretToken + '&queryHandle=' + handle;
      return BaseAdapter.get(queryUrl);
    }));
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
    return BaseAdapter.get(url + '?sessionid=' + secretToken + '&dbName=' + database);
  },

  getTableDetails (secretToken, tableName, database) {
    let url = baseUrl + urls.getTables + '/' + database + '.' + tableName;
    return BaseAdapter.get(url + '?sessionid=' + secretToken);
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
    return BaseAdapter.get(url + '?sessionid=' + secretToken);
  },

  getInMemoryResults (secretToken, handle) {
    let resultUrl = baseUrl + urls.query + '/' + handle + '/resultset';
    let results = BaseAdapter.get(resultUrl + '?sessionid=' + secretToken);

    let metaUrl = baseUrl + urls.query + '/' + handle + '/resultsetmetadata';
    let meta = BaseAdapter.get(metaUrl + '?sessionid=' + secretToken);

    return Promise.all([results, meta]);
  },

  getSavedQueries (secretToken, user, options = {}) {
    let url = baseUrl + urls.getSavedQueries;
    return BaseAdapter.get(
      url + '?user=' + user + '&sessionid=' + secretToken + '&start=' +
      (options.offset || 0) + '&count=' + (options.pageSize || 10)
    );
  },

  getParams (secretToken, query) {
    let url = baseUrl + urls.parameters + '?sessionid=' + secretToken;

    let formData = new FormData();
    formData.append('query', query);

    return BaseAdapter.post(url, formData);
  },

  runSavedQuery (secretToken, id, params) {
    let queryParamString = BaseAdapter.jsonToQueryParams(params);
    let url = baseUrl + urls.runSavedQuery + '/' + id + queryParamString;

    let formData = new FormData();
    formData.append('sessionid', secretToken);
    formData.append('conf',
      '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><conf></conf>');

    return BaseAdapter.post(url, formData);
  }
};

export default AdhocQueryAdapter;
