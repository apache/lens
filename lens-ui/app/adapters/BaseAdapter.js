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

import reqwest from 'qwest';
import Promise from 'bluebird';

import Config from 'config.json';
import XMLAdapter from './XMLAdapter';

function makeReqwest (url, method, data, options = {}) {
  let reqwestOptions = { headers: {}, timeout: 200000 }; // a large enough for native tables
  if (Config.headers) reqwestOptions.headers = Config.headers;
  reqwestOptions.responseType = !options.contentType ? 'json' : 'document';

  if (reqwestOptions.responseType !== 'document') {
    if (method === 'post' || method === 'put') reqwestOptions.dataType = 'json';
  } else {
    delete reqwestOptions.headers['Content-Type'];
  }

  return new Promise((resolve, reject) => {
    reqwest[method](url, data, reqwestOptions)
      .then((response) => {
        response = reqwestOptions.responseType === 'json' ?
          response.response :
          XMLAdapter.stringToXML(response.response);

        resolve(response);
      }, (error) => {
        let response = error.responseType !== 'json' ?
          XMLAdapter.stringToXML(error.response) :
          error.response;

        if (!response) {
          response = {
            status: error.status,
            statusText: error.statusText
          };
        }

        reject(response);
      }).catch(e => console.error(e));
  });
}

let BaseAdapter = {
  get (url, data, options) {
    return makeReqwest(url, 'get', data, options);
  },

  post (url, data, options = {}) {
    return makeReqwest(url, 'post', data, options);
  },

  put (url, data, options = {}) {
    return makeReqwest(url, 'put', data, options);
  },

  delete (url, data) {
    return makeReqwest(url, 'delete', data);
  },

  jsonToQueryParams (json) {
    // if json is an array?
    var queryParams = '?';
    if (!Object.prototype.toString.call(json).match('Array')) json = [json];

    json.forEach(object => {
      Object.keys(object).forEach(key => {
        queryParams += key + '=' + object[key] + '&';
      });
    });
    return queryParams.slice(0, -1);
  }
};

export default BaseAdapter;
