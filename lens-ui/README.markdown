Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.



# LENS UI #

This app is a front end client for Apache LENS server.
- It lets you fire queries.
- Discover cubes, its measures and dimensions.

# Running #

## Using Node Express Server ##
- add your LENS Server address in **package.json** under `scripts` *start* and *dev* to `lensserver` argument, this is the value of `lens.server.base.url` property of your LENS installation
- default port is 8082, if you'd like to change it please change the *port* argument in **package.json** under `scripts` *start* or *dev*
- doing ```npm run dev``` starts the UI in dev mode, with the JavaScript assets in a non-minified way which help in debugging, also it'll be watching to the changes in the source and would generate the new assets automatically.
- doing ```npm run start``` will minify and uglifiy the assets.

```bash
cd lens-ui
npm run start
```

- point chrome to [http://localhost:8082](http://localhost:8082)

## Using any other server ##

```bash
cd lens-ui
npm install
node_modules/webpack/bin/webpack
```
- you now have built assets in `assets` directory
- add LENS server address *(lens.server.base.url)* in `config.json` file in `baseURL`
- run your server, e.g python

```bash
python -m SimpleHTTPServer
```
- this will serve the `index.html` present at the root.
- this will cause your browser to make cross domain requests and if you don't have CORS enabled on your server, the app won't be able to get any data from LENS server.
  - to get around till you enable CORS on server side, open chrome in disabled web security mode.
  - first, quit chrome completely, then see the following steps to run browsers in disabled security mode
  - [start chrome in disabled security mode](https://blog.nraboy.com/2014/08/bypass-cors-errors-testing-apis-locally/)

# Code Structure #

- All JavaScript is in app directory.

# Configurations #

- The app can be configured to show either **INMEMORY** or **PERSISTENT** results.
- The setting can be done by altering a boolean variable present in `config.json`
  - `isPersistent`: true // results will be available as downloadable files
  - `isPersistent`: false // results will be shown in table format
- Any custom headers you wish to add can be added in `config.json` as a property
  ```javascript
  "headers": {
    "Some-Header": "SomeValue",
    "Another-Header": "AnotherValue"
  }
  ```

# Environment #

Built using:-

- React
- [Facebook's Flux implementation](https://www.npmjs.com/package/flux)
- [Reqwest](https://www.npmjs.com/package/reqwest) for making ajax calls wrapped with JavaScript promises.
