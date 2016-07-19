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

var express = require('express');
var path = require('path');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var session = require('express-session');

var app = express();
var httpProxy = require('http-proxy');
var proxy = httpProxy.createProxyServer();

var port = process.env.npm_config_port || 8082;

app.use(logger('dev'));
app.use(cookieParser());
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';
process.env.npm_config_lensserver = process.env.npm_config_lensserver || 'http://0.0.0.0:9999/lensapi/';
if (!process.env.npm_config_lensserver) {
  throw new Error('Specify LENS Server address in `lensserver` argument');
}
process.title = "lens-ui-server";
console.log('Using this as your LENS Server Address: ', process.env.npm_config_lensserver);
console.log('If this seems wrong, please edit `lensserver` argument in package.json. Do not forget to append http://\n');

app.use(session({
  secret            : 'SomethingYouKnow',
  resave            : false,
  saveUninitialized : true
}));

var fs = require('fs');
app.use(express.static(path.resolve(__dirname, 'target', 'assets')));

app.get('/health', function (req, res) {
  res.status(200).send('Adhoc Query UI is up and running.');
});

app.get('/target/assets/*', function (req, res) {
  res.setHeader('Cache-Control', 'public');
  res.end(fs.readFileSync(__dirname + req.path));
});

app.all('/serverproxy/*', function (req, res) {
  req.url = req.url.replace('serverproxy', '');
  proxy.web(req, res, {
    target: process.env.npm_config_lensserver
  }, function (e) {
    console.error('Proxy Error: ', e);
  });
});

app.get('*', function (req, res) {
  res.end(fs.readFileSync(__dirname + '/index.html'));
});
var server = app.listen(port, function (err) {
  if (err) throw err;

  console.log('Ad hoc UI server listening at %s', port);
});
