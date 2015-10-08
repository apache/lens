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
import { Link } from 'react-router';
import ClassNames from 'classnames';

class QueryOperations extends React.Component {
  constructor () {
    super();
    this.state = { isCollapsed: false };
    this.toggle = this.toggle.bind(this);
  }

  toggle () {
    this.setState({ isCollapsed: !this.state.isCollapsed });
  }

  render () {
    let collapseClass = ClassNames({
      'pull-right': true,
      'glyphicon': true,
      'glyphicon-chevron-up': !this.state.isCollapsed,
      'glyphicon-chevron-down': this.state.isCollapsed
    });

    let panelBodyClassName = ClassNames({
      'panel-body': true,
      'hide': this.state.isCollapsed
    });

    return (
      <div className='panel panel-default'>
        <div className='panel-heading'>
          <h3 className='panel-title'>
            Queries
            <span className={collapseClass} onClick={this.toggle}></span>
          </h3>
        </div>
        <div className={panelBodyClassName}>
          <ul style={{listStyle: 'none', paddingLeft: '0px',
            marginBottom: '0px'}}>
            <li><Link to='results'>All</Link></li>
            <li>
              <Link to='results' query={{category: 'running'}}>
                Running
              </Link>
            </li>
            <li>
              <Link to='results' query={{category: 'successful'}}>
                Completed
              </Link>
            </li>
            <li>
              <Link to='results' query={{category: 'queued'}}>
                Queued
              </Link>
            </li>
            <li>
              <Link to='results' query={{category: 'failed'}}>
                Failed
              </Link>
            </li>
            <li>
              <Link to='savedqueries'>
                Saved Queries
              </Link>
            </li>
          </ul>
        </div>
      </div>
    );
  }
}

export default QueryOperations;
