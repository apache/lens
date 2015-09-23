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
import { RouteHandler } from 'react-router';

import QueryBox from './QueryBoxComponent';
import Sidebar from './SidebarComponent';
import RequireAuthentication from './RequireAuthenticationComponent';

class AdhocQuery extends React.Component {
  constructor (props) {
    super(props);
    this.state = {toggleQueryBox: true}; // show box when true, hide on false
    this.toggleQueryBox = this.toggleQueryBox.bind(this);
  }

  render() {
    let toggleButtonClass = this.state.toggleQueryBox ? 'default' : 'primary';

    return (
      <section className="row">
        <div className="col-md-4">
          <Sidebar />
        </div>

        <div className="col-md-8">
          <div className="panel panel-default">
            <div className="panel-heading">
              <h3 className="panel-title">
                Compose
                <button
                  className={'btn btn-xs pull-right btn-' + toggleButtonClass}
                  onClick={this.toggleQueryBox}>
                  {this.state.toggleQueryBox ? 'Hide': 'Show'} Query Box
                </button>
              </h3>
            </div>
            <div className="panel-body" style={{padding: '0px'}}>
              <QueryBox toggleQueryBox={this.state.toggleQueryBox} {...this.props}/>
            </div>
          </div>

          <RouteHandler toggleQueryBox={this.state.toggleQueryBox}/>
        </div>
      </section>
    );
  }

  // FIXME persist the state in the URL as well
  toggleQueryBox () {
    this.setState({toggleQueryBox: !this.state.toggleQueryBox});
  }
};

let AuthenticatedAdhocQuery = RequireAuthentication(AdhocQuery);

export default AuthenticatedAdhocQuery;
