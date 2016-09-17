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
import UserStore from '../stores/UserStore';
import AdhocQueryActions from '../actions/AdhocQueryActions';
import QueryPreview from './QueryPreviewComponent';

// this method fetches the results based on props.query.state
function getResults(query) {
  AdhocQueryActions
    .getQueryHandles(UserStore.getUserDetails().secretToken, UserStore.getUserDetails().email, query);
}

function getQueryHandles() {
  return AdhocQueryStore.getQueryHandles();
}

class QueryResults extends React.Component {
  constructor(props) {
    super(props);
    this.state = {queries: {}, queriesReceived: false};
    this._onChange = this._onChange.bind(this);
    this.adjustRange = this.adjustRange.bind(this);
    getResults(props.query);
  }

  componentDidMount() {
    AdhocQueryStore.addChangeListener(this._onChange);
  }

  componentWillUnmount() {
    AdhocQueryStore.removeChangeListener(this._onChange);
  }

  componentWillReceiveProps(props) {
    getResults(props.query);
    this.setState({queries: {}, queriesReceived: false});
  }
  fetchDetailsAndSetTimeout(handles) {
    AdhocQueryActions.getQueriesDetails(UserStore.getUserDetails().secretToken, handles);
  }
  adjustRange(event) {
    event.preventDefault();
    let query = JSON.parse(JSON.stringify(this.props.query));
    if (this.refs.fromDate.getDOMNode().value) {
      query.fromDate = this.refs.fromDate.getDOMNode().value;
    } else {
      delete query['fromDate'];
    }
    if (this.refs.toDate.getDOMNode().value) {
      query.toDate = this.refs.toDate.getDOMNode().value;
    } else {
      delete query['toDate'];
    }
    var { router } = this.context;
    router.transitionTo('results', {}, query);
  }

  render() {
    let queries = '';

    let queryMap = this.state.queries;
    let queriesToRefresh = []
    queries = Object.keys(queryMap)
      .map((queryHandle) => {
        let query = queryMap[queryHandle];
        if (query.status.status == "RUNNING" || query.status.status == "QUEUED") {
          queriesToRefresh.push(query.queryHandle.handleId);
        }
        return (
          <QueryPreview key={query.queryHandle.handleId} {...query} />
        );
      }); // end of map

    // FIXME find a better way to do it.
    // show a loader when queries are empty, or no queries.
    // this is managed by seeing the length of queries and
    // a state variable 'queriesReceived'.
    // if queriesReceived is true and the length is 0, show no queries else
    // show a loader
    let queriesLength = Object.keys(this.state.queries).length;

    if (!queriesLength && !this.state.queriesReceived) {
      queries = <Loader size='8px' margin='2px'/>;
    } else if (!queriesLength && this.state.queriesReceived) {
      queries = <div className='alert alert-danger'>
        <strong>Sorry</strong>, there were no queries to be shown.
      </div>;
    }
    if (queriesToRefresh && queriesToRefresh.length) {
      // refresh in 5 seconds
      setTimeout(this.fetchDetailsAndSetTimeout, 5000, queriesToRefresh);
    }
    return (
      <section>
        <div style={{border: '1px solid #dddddd', borderRadius: '4px',
          padding: '0px 8px 8px 8px'}}>
          <h3 style={{margin: '8px 10px'}}>Results</h3>
          <form className='form-range' onSubmit={this.adjustRange}>
            <input ref='fromDate' required={false} defaultValue={this.props.query.fromDate} id='fromDate'
                   placeholder='now-30years' autoFocus/>
            <input ref='toDate' required={false} defaultValue={this.props.query.toDate} id='toDate' placeholder='now'
                   autoFocus/>
            <button className='btn btn-primary' type='submit'>Fetch</button>
          </form>
          <hr style={{marginTop: '6px'}}/>
          <div>
            {queries}
          </div>
        </div>
      </section>
    );
  }

  _onChange() {
    let handles = getQueryHandles();
    let queries = handles.map((handle) => (
      AdhocQueryStore.getQueryDetails(handle) || {
        "queryHandle": {
          "handleId": handle
        },
        "userQuery": handle,
        //"submittedUser": undefined,
        //"priority": "VERY_LOW",
        //"isPersistent": true,
        //"selectedDriverName": "hive/prod",
        //"driverQuery": "cube select ...",
        "status": {
          "progress": -1.0,
          "status": "UNKNOWN"
          //"isResultSetAvailable": false,
          //"errorMessage": "Query execution failed!"
        },
        //"queryConf": {},
        //"submissionTime": 1468403280197,
        //"launchTime": 1468403284328,
        //"driverStartTime": 1468403280581,
        //"driverFinishTime": 1468403350769,
        //"finishTime": 1468403373582,
        //"closedTime": 0,
        "queryName": "Loading..."
      }
    ));
    this.setState({queries: queries, queriesReceived: true});
    AdhocQueryActions.getQueriesDetails(UserStore.getUserDetails().secretToken,
      handles.filter(handle=>(!(AdhocQueryStore.getQueryDetails(handle)))));
  }
}

QueryResults.contextTypes = {
  router: React.PropTypes.func
};


export default QueryResults;
