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
import ClassNames from 'classnames';

import DatabaseStore from '../stores/DatabaseStore';
import AdhocQueryActions from '../actions/AdhocQueryActions';
import UserStore from '../stores/UserStore';
import Loader from '../components/LoaderComponent';
import CubeTree from './CubeTreeComponent';
import TableTree from './TableTreeComponent';
import Config from 'config.json';

function getDatabases () {
  return DatabaseStore.getDatabases();
}

class DatabaseComponent extends React.Component {
  constructor (props) {
    super(props);
    this.state = {
      databases: [],
      loading: true,
      isCollapsed: false,
      selectedDatabase: UserStore.getUserDetails().database
    };
    this._onChange = this._onChange.bind(this);
    this.toggle = this.toggle.bind(this);
    this.setDatabase = this.setDatabase.bind(this);

    AdhocQueryActions.getDatabases(UserStore.getUserDetails().secretToken);
  }

  componentDidMount () {
    DatabaseStore.addChangeListener(this._onChange);
    UserStore.addChangeListener(this._onChange);
  }

  componentWillUnmount () {
    DatabaseStore.removeChangeListener(this._onChange);
    UserStore.removeChangeListener(this._onChange);
  }

  render () {
    let databaseComponent = null;

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

    databaseComponent = (<div>
        <label className='control-label' id='db'>Select a Database</label>
        <select className='form-control' id='db' onChange={this.setDatabase} value={this.state.selectedDatabase} >
          <option value=''>Select</option>
          {this.state.databases.map(database => {
            return <option key={database} value={database}>{database}</option>;
          })}
        </select>
      </div>);


    if (this.state.loading) {
      databaseComponent = <Loader size='4px' margin='2px'/>;
    } else if (!this.state.databases.length) {
      databaseComponent = (<div className='alert-danger'
                                style={{padding: '8px 5px'}}>
        <strong>Sorry, we couldn&#39;t find any databases.</strong>
      </div>);
    }
    return (<div>
        {databaseComponent}
        {
          this.state.selectedDatabase &&
          <div>
            <hr style={{marginTop: '10px', marginBottom: '10px'}}/>
            <CubeTree key={this.state.selectedDatabase}
                      database={this.state.selectedDatabase}/>
          </div>
        }
        {
          this.state.selectedDatabase && Config.display_tables &&
          <div>
            <hr style={{marginTop: '10px', marginBottom: '10px'}}/>
            <TableTree key={this.state.selectedDatabase}
                       database={this.state.selectedDatabase}/>
          </div>
        }

      </div>
    );
  }

  _onChange () {
    this.setState({ databases: getDatabases(), loading: false, selectedDatabase:  UserStore.currentDatabase() });
  }

  toggle () {
    this.setState({ isCollapsed: !this.state.isCollapsed });
  }

  setDatabase(event) {
    var dbName = null;
    if (typeof(event) == "string") {
      dbName = event;
    } else {
      dbName = event.target.value;
    }
    AdhocQueryActions.setDatabase(UserStore.getUserDetails().secretToken, dbName);
  }
}

export default DatabaseComponent;
