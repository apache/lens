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

import AdhocQueryActions from '../actions/AdhocQueryActions';
import SavedQueryStore from '../stores/SavedQueryStore';
import UserStore from '../stores/UserStore';
import Loader from './LoaderComponent';
import SavedQueryPreview from './SavedQueryPreviewComponent';

class SavedQueries extends React.Component {
  constructor (props) {
    super(props);
    this.state = {
      loading: true,
      savedQueries: null,
      page: null,
      totalPages: null
    };

    this._onChange = this._onChange.bind(this);
    this.prev = this.prev.bind(this);
    this.next = this.next.bind(this);
    this._getPaginatedSavedQueries = this._getPaginatedSavedQueries.bind(this);

    let secretToken = UserStore.getUserDetails().secretToken;
    let user = UserStore.getUserDetails().email;
    AdhocQueryActions.getSavedQueries(secretToken, user);
  }

  componentDidMount () {
    SavedQueryStore.addChangeListener(this._onChange);
  }

  componentWillUnmount () {
    SavedQueryStore.removeChangeListener(this._onChange);
  }

  render () {
    let loading = this.state.loading ? <Loader size='4px' margin='2px'/> : null;

    let queries = !this.state.savedQueries ? null :
      Object.keys(this.state.savedQueries).map(queryId => {
        return <SavedQueryPreview key={'saved|' + queryId}
          query={this.state.savedQueries[queryId]} />;
      });

    // no saved queries
    if (!this.state.loading && !queries) {
      queries = (<div className='alert-danger' style={{padding: '8px 5px'}}>
          <strong>Sorry, we couldn&#39;t find any saved queries.</strong>
        </div>);
    }

    if (!this.state.loading && this.state.totalPages == 0) {
      queries = (<div className='alert-danger' style={{padding: '8px 5px'}}>
          <strong>You&#39;ve not saved any query.</strong>
        </div>);
    }

    var pagination = (
      <div className='pull-right'>
        <button onClick={this.prev}
          className='btn btn-link glyphicon glyphicon-triangle-left'>
        </button>
          <small>
            { this.state.page && this.state.totalPages &&
              (this.state.page + ' of ' + this.state.totalPages)
            }
          </small>
        <button onClick={this.next}
          className='btn btn-link glyphicon glyphicon-triangle-right'>
        </button>
      </div>
    );

    return (
      <section>
        <div style={{border: '1px solid #dddddd', borderRadius: '4px',
          padding: '0px 8px 8px 8px'}}>
          <h3 style={{margin: '8px 10px'}}>
            Saved Queries
            {pagination}
          </h3>
          <hr style={{marginTop: '6px'}}/>
          <div>
            {loading}
            {queries}
          </div>
        </div>
      </section>
    );
  }

  prev () {
    if (this.state.page > 1) this._onChange(this.state.page - 1);
  }

  next () {
    if (this.state.page < this.state.totalPages) {
      this._onChange(this.state.page + 1);
    }
  }

  _onChange (page) {
    // done to filter success/error messages from store
    if (typeof page === 'object') page = this.state.page;

    var PAGE_SIZE = 10;
    page = page || this.state.page || 1;
    var state = {
      savedQueries: this._getPaginatedSavedQueries(page, PAGE_SIZE),
      page: page,
      totalPages: Math.ceil(SavedQueryStore.getTotalRecords() / PAGE_SIZE)
    };
    state.loading = !!(!state.savedQueries && state.totalPages);
    this.setState(state);
  }

  _getPaginatedSavedQueries (pageNumber, pageSize) {
    if (!pageNumber && !pageSize) return;

    var token = UserStore.getUserDetails().secretToken;
    var email = UserStore.getUserDetails().email;
    var savedQueries = SavedQueryStore.getSavedQueries();
    var savedQueriesLength = savedQueries && Object.keys(savedQueries).length;
    var totalQueries = SavedQueryStore.getTotalRecords();
    var relevantSavedQueries = null;
    var startIndex = (pageNumber - 1) * pageSize;
    if (savedQueriesLength > startIndex) {
      relevantSavedQueries = Object.keys(savedQueries)
        .slice(startIndex, startIndex + pageSize);
      if ((totalQueries != savedQueriesLength) && (relevantSavedQueries.length < pageSize) && totalQueries) {
        // call backend
        AdhocQueryActions.getSavedQueries(token, email, {
          offset: startIndex,
          pageSize: pageSize
        });

        this.setState({loading: true});
      }
    } else {
      // trigger action
      if (!totalQueries) return;

      AdhocQueryActions.getSavedQueries(token, email, {
        offset: startIndex,
        pageSize: pageSize
      });

      this.setState({loading: true});
    }

    var filteredQueries = relevantSavedQueries && relevantSavedQueries.map(id => {
      return savedQueries[id];
    });

    return filteredQueries;
  }

}

export default SavedQueries;
