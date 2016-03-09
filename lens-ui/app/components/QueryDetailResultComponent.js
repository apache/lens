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

import Loader from './LoaderComponent';
import AdhocQueryStore from '../stores/AdhocQueryStore';
import AdhocQueryActions from '../actions/AdhocQueryActions';
import UserStore from '../stores/UserStore';
import QueryPreview from './QueryPreviewComponent';

let interval = null;

function isResultAvailableOnServer (handle) {
  // always check before polling
  let query = AdhocQueryStore.getQueries()[handle];
  if (query && query.status && query.status.status === 'SUCCESSFUL') {
    return true;
  }
  return false;
}

function fetchResult (secretToken, handle) {
  if (isResultAvailableOnServer(handle)) {
    let query = AdhocQueryStore.getQueries()[handle];
    let mode = query.isPersistent ? 'PERSISTENT' : 'INMEMORY';
    AdhocQueryActions.getQueryResult(secretToken, handle, mode);
  } else {
    AdhocQueryActions.getQuery(secretToken, handle);
  }
}

function constructTable (tableData) {
  if (!tableData.columns && !tableData.results) return;
  let header = tableData.columns.map(column => {
    return <th>{ column.name }</th>;
  });
  let rows = tableData.results
    .map(row => {
      return (<tr>{row.values.map(cell => {
        return <td>{(cell && cell.value) || <span style={{color: 'red'}}>NULL</span>}</td>;
      })}</tr>);
    });

  // in case the results are empty, happens when LENS server has restarted
  // all in-memory results are wiped clean
  if (!rows.length) {
    let colWidth = tableData.columns.length;
    rows = <tr>
        <td colSpan={colWidth} style={{color: 'red', textAlign: 'center'}}>
          Result set no longer available with server.</td>
      </tr>;
  }

  return (
    <div className='table-responsive'>
      <table className='table table-striped table-condensed'>
        <thead>
          <tr>{header}</tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
  );
}

class QueryDetailResult extends React.Component {
  constructor (props) {
    super(props);
    this.state = { loading: true, queryResult: {}, query: null };
    this._onChange = this._onChange.bind(this);
    this.pollForResult = this.pollForResult.bind(this);
  }

  componentDidMount () {
    let secretToken = UserStore.getUserDetails().secretToken;
    this.pollForResult(secretToken, this.props.params.handle);

    AdhocQueryStore.addChangeListener(this._onChange);
  }

  componentWillUnmount () {
    clearInterval(interval);
    AdhocQueryStore.removeChangeListener(this._onChange);
  }

  componentWillReceiveProps (props) {
    this.state = { loading: true, queryResult: {}, query: null };
    let secretToken = UserStore.getUserDetails().secretToken;
    clearInterval(interval);
    this.pollForResult(secretToken, props.params.handle);
  }

  render () {
    let query = this.state.query;
    let queryResult = this.state.queryResult;
    let result = '';

    // check if the query was persistent or in-memory
    if (query && query.isPersistent && query.status.status === 'SUCCESSFUL') {
      result = (<div className='text-center'>
        <a href={queryResult.downloadURL} download>
          <span className='glyphicon glyphicon-download-alt	'></span> Click
          here to download the results as a CSV file
        </a>
      </div>);
    } else {
      result = constructTable(this.state.queryResult);
    }

    if (this.state.loading) result = <Loader size='8px' margin='2px' />;

    return (
      <div className='panel panel-default'>
      <div className='panel-heading'>
        <h3 className='panel-title'>Query Result</h3>
      </div>
      <div className='panel-body no-padding'>
        <div>
          <QueryPreview key={query && query.queryHandle.handleId}
            {...query} />
        </div>
        {result}
      </div>
    </div>
    );
  }

  pollForResult (secretToken, handle) {
    // fetch results immediately if present, don't wait for 5 seconds
    // in setInterval below.
    // FIXME if I put a return in if construct, setInterval won't execute which
    // shouldn't but the backend API isn't stable enough, and if this call fails
    // we'll not be able to show the results and it'll show a loader, thoughts?
    fetchResult(secretToken, handle);

    interval = setInterval(function () {
      fetchResult(secretToken, handle);
    }, 5000);
  }

  _onChange () {
    let handle = this.props.params.handle;
    let query = AdhocQueryStore.getQueries()[handle];
    let result = AdhocQueryStore.getQueryResult(handle);
    let loading = true;

    let failed = query && query.status && query.status.status === 'FAILED';
    let success = query && query.status && query.status.status === 'SUCCESSFUL';

    if (failed || success && result) {
      clearInterval(interval);
      loading = false;
    }

    // check first if the query failed, clear the interval, and show it
    // setState when query is successful AND we've the results OR it failed
    let state = {
      loading: loading,
      queryResult: result || {}, // result can be undefined so guarding it
      query: query
    };

    this.setState(state);
  }
}

QueryDetailResult.propTypes = {
  query: React.PropTypes.object,
  params: React.PropTypes.object
};

export default QueryDetailResult;
