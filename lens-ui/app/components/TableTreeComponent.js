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
import TreeView from 'react-treeview';
import { Link } from 'react-router';
import 'react-treeview/react-treeview.css';

import TableStore from '../stores/TableStore';
import AdhocQueryActions from '../actions/AdhocQueryActions';
import UserStore from '../stores/UserStore';
import Loader from './LoaderComponent';
import '../styles/css/tree.css';

let filterString = '';

function getState (page, filterString, database) {
  let state = getTables(page, filterString, database);
  state.page = page;
  state.loading = false;
  return state;
}

function getTables (page, filterString, database) {
  // get all the native tables
  // so that Object.keys does not throw up
  let tables = TableStore.getTables(database) || {};
  let pageSize = 10;
  let allTables;
  let startIndex;
  let relevantIndexes;
  let pageTables;

  if (!filterString) {
    // no need for filtering
    allTables = Object.keys(tables);
  } else {
    // filter
    allTables = Object.keys(tables).map(name => {
      if (name.match(filterString)) return name;
    }).filter(name => { return !!name; });
  }

  startIndex = (page - 1) * pageSize;
  relevantIndexes = allTables.slice(startIndex, startIndex + pageSize);
  pageTables = relevantIndexes.map(name => {
    return tables[name];
  });

  return {
    totalPages: Math.ceil(allTables.length / pageSize),
    tables: pageTables
  };
}

class TableTree extends React.Component {
  constructor (props) {
    super(props);
    this.state = {
      tables: [],
      totalPages: 0,
      page: 0,
      loading: true,
      isCollapsed: false
    };
    this._onChange = this._onChange.bind(this);
    this.prevPage = this.prevPage.bind(this);
    this.nextPage = this.nextPage.bind(this);
    this.toggle = this.toggle.bind(this);
    this.validateClickEvent = this.validateClickEvent.bind(this);

    if (!TableStore.getTables(props.database)) {
      AdhocQueryActions
        .getTables(UserStore.getUserDetails().secretToken, props.database);
    } else {
      let state = getState(1, '', props.database);
      this.state = state;

      // on page refresh only a single table is fetched, and hence we need to
      // fetch others too.
      if (!TableStore.areTablesCompletelyFetched(props.database)) {
        AdhocQueryActions
          .getTables(UserStore.getUserDetails().secretToken, props.database);
      }
    }
  }

  componentDidMount () {
    TableStore.addChangeListener(this._onChange);

    // listen for opening tree
    this.refs.tableTree.getDOMNode()
      .addEventListener('click', this.validateClickEvent);
  }

  componentWillUnmount () {
    this.refs.tableTree.getDOMNode()
      .removeEventListener('click', this.validateClickEvent);
    TableStore.removeChangeListener(this._onChange);
  }

  render () {
    let tableTree = '';

    // construct tree
    tableTree = this.state.tables.map(table => {
      let label = (<Link to='tableschema' params={{tableName: table.name}}
        title={table.name} query={{database: this.props.database}}>
          {table.name}</Link>);
      return (
        <TreeView key={table.name} nodeLabel={label}
          defaultCollapsed={!table.isLoaded}>

          {table.isLoaded ? table.columns.map(col => {
            return (
              <div className='treeNode' key={table.name + '|' + col.name}>
                {col.name} ({col.type})
              </div>
            );
          }) : <Loader size='4px' margin='2px' />}

        </TreeView>
      );
    });

    // show a loader when tree is loading
    if (this.state.loading) {
      tableTree = <Loader size='4px' margin='2px' />;
    } else if (!this.state.tables.length) {
      tableTree = (<div className='alert-danger' style={{padding: '8px 5px'}}>
          <strong>Sorry, we couldn&#39;t find any.</strong>
        </div>);
    }

    let pagination = this.state.tables.length ?
      (
        <div>
          <div className='text-center'>
            <button className='btn btn-link glyphicon glyphicon-triangle-left page-back'
              onClick={this.prevPage}>
            </button>
            <span>{this.state.page} of {this.state.totalPages}</span>
            <button className='btn btn-link glyphicon glyphicon-triangle-right page-next'
              onClick={this.nextPage}>
            </button>
          </div>
        </div>
      ) : null;

    return (
      <div>
        { !this.state.loading &&
          <div className='form-group'>
            <input type='search' className='form-control'
              placeholder='Type to filter tables'
              onChange={this._filter.bind(this)}/>
          </div>
        }

        {pagination}

        <div ref='tableTree' style={{maxHeight: '350px', overflowY: 'auto'}}>
          {tableTree}
        </div>
      </div>
    );
  }

  _onChange (page) {
    // so that page doesn't reset to beginning
    page = page || this.state.page || 1;
    this.setState(getState(page, filterString, this.props.database));
  }

  getDetails (tableName, database) {
    // find the table
    let table = this.state.tables.filter(table => {
      return tableName === table.name;
    });

    if (table.length && table[0].isLoaded) return;

    AdhocQueryActions
      .getTableDetails(UserStore.getUserDetails().secretToken, tableName,
        database);
  }

  _filter (event) {
    filterString = event.target.value;
    this._onChange();
  }

  prevPage () {
    if (this.state.page - 1) this._onChange(this.state.page - 1);
  }

  nextPage () {
    if (this.state.page < this.state.totalPages) {
      this._onChange(this.state.page + 1);
    }
  }

  toggle () {
    this.setState({ isCollapsed: !this.state.isCollapsed });
  }

  validateClickEvent (e) {
    if (e.target && e.target.nodeName === 'DIV' &&
      e.target.nextElementSibling.nodeName === 'A') {
      this.getDetails(e.target.nextElementSibling.textContent, this.props.database);
    }
  }

}

TableTree.propTypes = {
  database: React.PropTypes.string.isRequired
};

export default TableTree;
