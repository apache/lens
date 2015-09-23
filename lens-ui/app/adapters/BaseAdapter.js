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

import reqwest from 'reqwest';
import Promise from 'bluebird';

import Config from 'config.json';

function makeReqwest (url, method, data, options = {}) {
  let reqwestOptions = {
    url: url,
    method: method,
    contentType: 'application/json',
    type: 'json',
    headers: {}
  };

  if (Config.headers) reqwestOptions.headers = Config.headers;

  // delete Content-Type and add Accept
  reqwestOptions.headers['Accept'] = 'application/json';
  delete reqwestOptions.headers['Content-Type'];
  if (data) reqwestOptions.data = data;
  if (options.contentType === 'multipart/form-data') {
    reqwestOptions.processData = false;
    reqwestOptions.contentType = 'multipart/form-data';

    // because server can't handle JSON response on POST
    delete reqwestOptions.type;
    delete reqwestOptions.headers['Accept'];
  }

  return new Promise ((resolve, reject) => {
    reqwest(reqwestOptions)
      .then ((response) => {
        resolve(response);
      }, (error) => {
        reject(error);
      });
  });
}

function deleteRequest (url, dataArray) {
  return makeReqwest(url, 'delete', dataArray);
}

function get (url, dataArray, options) {
  return makeReqwest(url, 'get', dataArray, options);
}

// TODO need to fix this unused 'options'. What params can it have?
function postJson (url, data, options = {}) {
  return makeReqwest(url, 'post', data, {contentType: 'application/json'});
}

function postFormData (url, data, options = {}) {
  return makeReqwest(url, 'post', data, options);
}

let BaseAdapter = {
  get: get,

  post (url, data, options = {}) {
    if (options.contentType) {
      return postFormData(url, data, options);
    } else {
      return postJson(url, data, options);
    }
  },

  delete: deleteRequest
};

export default BaseAdapter;
