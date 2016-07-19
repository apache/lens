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

import BaseAdapter from './BaseAdapter';
import Config from 'config.json';

// these are required by lens API. Sad.
let authUrl = Config.baseURL + 'session';
let queryMode = Config.isPersistent;
let sessionconf = `<?xml version="1.0" encoding="UTF-8"?>
                  <conf>
                    <properties>
                      <entry>
                        <key>lens.query.enable.persistent.resultset</key>
                        <value>` + queryMode + `</value>
                       </entry>
                       <entry>
                        <key>lens.query.enable.persistent.resultset.indriver</key>
                        <value>false</value>
                       </entry>
                    </properties>
                  </conf>`;

let AuthenticationAdapter = {
  authenticate (email, password, database) {

    // preparing data as API accepts multipart/form-data :(
    var formData = new FormData();
    formData.append('username', email);
    formData.append('password', password || "");
    formData.append('database', database|| "default");
    formData.append('sessionconf', sessionconf);

    return BaseAdapter.post(authUrl, formData, {
      contentType: 'multipart/form-data'
    });
  }
};

export default AuthenticationAdapter;
