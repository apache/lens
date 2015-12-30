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

// this method fetches the results based on props.query.category
function getResults (props) {
  let email = UserStore.getUserDetails().email;
  let secretToken = UserStore.getUserDetails().secretToken;

  if (props.query.category) {
    // fetch either running or completed results
    AdhocQueryActions
      .getQueries(secretToken, email, { state: props.query.category });
  } else {
    // fetch all
    AdhocQueryActions.getQueries(secretToken, email);
  }
}

function getQueries () {
  return AdhocQueryStore.getQueries();
}

class QueryResults extends React.Component {
  constructor (props) {
    super(props);
    this.state = { queries: {}, queriesReceived: false };
    this._onChange = this._onChange.bind(this);

    getResults(props);
  }

  componentDidMount () {
    AdhocQueryStore.addChangeListener(this._onChange);
  }

  componentWillUnmount () {
    AdhocQueryStore.removeChangeListener(this._onChange);
  }

  componentWillReceiveProps (props) {
    getResults(props);
    this.setState({queries: {}, queriesReceived: false});
  }

  render () {
    let queries = '';

    let queryMap = this.state.queries;
    queries = Object.keys(queryMap)
      .sort(function (a, b) {
        return queryMap[b].submissionTime - queryMap[a].submissionTime;
      })
      .map((queryHandle) => {
        let query = queryMap[queryHandle];

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
      queries = <Loader size='8px' margin='2px' />;
    } else if (!queriesLength && this.state.queriesReceived) {
      queries = <div className='alert alert-danger'>
        <strong>Sorry</strong>, there were no queries to be shown.
      </div>;
    }

    return (
      <section>
        <div style={{border: '1px solid #dddddd', borderRadius: '4px',
          padding: '0px 8px 8px 8px'}}>
          <h3 style={{margin: '8px 10px'}}>Results</h3>
          <hr style={{marginTop: '6px'}}/>
          <div>
            {queries}
          </div>
        </div>
      </section>
    );
  }

  _onChange () {
    this.setState({queries: getQueries(), queriesReceived: true});
  }
}

export default QueryResults;
