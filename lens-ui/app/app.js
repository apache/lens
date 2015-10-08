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

import React from 'react';
import Router from 'react-router';
import { DefaultRoute, Route } from 'react-router';

import './styles/less/globals.less';
import './styles/css/global.css';

import Login from './components/LoginComponent';
import Logout from './components/LogoutComponent';
import About from './components/AboutComponent';
import App from './components/AppComponent';
import AdhocQuery from './components/AdhocQueryComponent';
import QueryResults from './components/QueryResultsComponent';
import CubeSchema from './components/CubeSchemaComponent';
import QueryDetailResult from './components/QueryDetailResultComponent';
import TableSchema from './components/TableSchemaComponent';
import SavedQueries from './components/SavedQueriesComponent';

let routes = (
  <Route name='app' path='/' handler={App} >
    <Route name='login' handler={Login}/>
    <Route name='logout' handler={Logout}/>
    <Route name='query' path='query' handler={AdhocQuery} >
      <Route name='results' handler={QueryResults}/>
      <Route name='savedqueries' handler={SavedQueries}/>
      <Route name='result' path='/results/:handle' handler={QueryDetailResult}/>
      <Route name='cubeschema' path='schema/cube/:cubeName' handler={CubeSchema}/>
      <Route name='tableschema' path='schema/table/:tableName'
        handler={TableSchema}/>

    </Route>
    <Route name='about' handler={About} />
    <DefaultRoute handler={AdhocQuery} />
  </Route>
);

Router.run(routes, Router.HistoryLocation, (Handler) => {
  React.render(<Handler/>, document.getElementById('app'));

  // and hide the loader which was loading in html while JavaScript
  // was downloading
  document.getElementById('loader-no-js').style.display = 'none';
});
