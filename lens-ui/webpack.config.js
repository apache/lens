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

var webpack = require('webpack');
var path = require('path');

module.exports = {

  entry: {
    app: [
      './app/app.js'
    ]
  },

  output: {
    path: path.join(__dirname, 'target', 'assets'),
    filename: 'bundle.js'
  },

  plugins: [
    new webpack.NoErrorsPlugin()
  ],

  devtool: 'source-map',

  resolve: {
    modulesDirectories: ['app', 'node_modules', __dirname]
  },

  module: {
    loaders: [
      { test: /\.jsx?$/, loaders: ['babel'], include: path.join(__dirname, 'app') },
      { test: /\.css$/, loaders: ['style', 'css'] },
      { test: /\.less$/, loaders: ['style', 'css', 'autoprefixer', 'less'] },
      { test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/, loaders: ['url?limit=10000&minetype=application/font-woff'] },
      { test: /\.(ttf|eot|svg)(\?v=[0-9]\.[0-9]\.[0-9])?$/, loaders: ['file'] },
      { test: /\.json$/, loaders: ['json'] },
      { test: /\.gif$/, loader: 'url-loader?mimetype=image/png' }
    ]
  }
};
