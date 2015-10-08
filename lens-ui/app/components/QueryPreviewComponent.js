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
import Moment from 'moment';
import { Link } from 'react-router';
import CodeMirror from 'codemirror';
import 'codemirror/mode/sql/sql.js';
import 'codemirror/addon/runmode/runmode.js';

import UserStore from '../stores/UserStore';
import AdhocQueryActions from '../actions/AdhocQueryActions';

class QueryPreview extends React.Component {
  constructor (props) {
    super(props);
    this.state = {showDetail: false};
    this.toggleQueryDetails = this.toggleQueryDetails.bind(this);
    this.cancelQuery = this.cancelQuery.bind(this);
  }

	render () {
    let query = this.props;

    if (!query.userQuery) return null;

    // code below before the return prepares the data to render, turning out
    // crude properties to glazed, garnished ones e.g. formatting of query
    let codeTokens = [];

    CodeMirror
      .runMode(query.userQuery,
        'text/x-mysql', function (text, style) {
        // this method is called for every token and gives the
        // token and style class for it.
        codeTokens.push(<span className={'cm-' + style}>{text}</span>);
      });

    // figuring out the className for query status
    // TODO optimize this construct
    let statusTypes = {
      'EXECUTED': 'success',
      'SUCCESSFUL': 'success',
      'FAILED': 'danger',
      'CANCELED': 'danger',
      'CLOSED': 'warning',
      'QUEUED': 'info',
      'RUNNING': 'info'
    };

    let statusClass = 'label-' + statusTypes[query.status.status] ||
      'label-info';
    let handle = query.queryHandle.handleId;
    let executionTime = (query.finishTime - query.submissionTime) / (1000 * 60);
    let statusType = query.status.status === 'ERROR' ? 'Error: ' : 'Status: ';
    let seeResult = '';
    let statusMessage = query.status.status === 'SUCCESSFUL' ?
      query.status.statusMessage :
      query.status.errorMessage;

    if (query.status.status === 'SUCCESSFUL') {
      seeResult = (<Link to='result' params={{handle: handle}}
        className='btn btn-success btn-xs pull-right' style={{marginLeft: '5px'}}>
        See Result
      </Link>);
    }

    return (
      <section>
        <div className='panel panel-default'>
          <pre className='cm-s-default' style={{cursor: 'pointer',
            border: '0px', marginBottom: '0px'}}
            onClick={this.toggleQueryDetails}>

            {codeTokens}

            <label className={'pull-right label ' + statusClass}>
              {query.status.status}
            </label>

            {query.queryName && (
              <label className='pull-right label label-primary'
                style={{marginRight: '5px'}}>
                {query.queryName}
              </label>
            )}

          </pre>

          {this.state.showDetail && (
            <div className='panel-body' style={{borderTop: '1px solid #cccccc',
            paddingBottom: '0px'}} key={'preview' + handle}>
              <div className='row'>
                <div className='col-lg-4 col-sm-4'>
                  <span className='text-muted'>Name </span>
                  <strong>{ query.queryName || 'Not specified'}</strong>
                </div>
                <div className='col-lg-4 col-sm-4'>
                  <span className='text-muted'>Submitted </span>
                  <strong>
                    { Moment(query.submissionTime).format('Do MMM YY, hh:mm:ss a')}
                  </strong>
                </div>
                <div className='col-lg-4 col-sm-4'>
                  <span className='text-muted'>Execution time </span>
                  <strong>

                    { executionTime > 0 ?
                      Math.ceil(executionTime) +
                        (executionTime > 1 ? ' mins' : ' min') : 'Still running'
                    }
                  </strong>
                </div>
              </div>
              <div className='row'>
                <div
                  className={'alert alert-' + statusTypes[query.status.status]}
                  style={{marginBottom: '0px', padding: '5px 15px 5px 15px'}}>
                    <p>
                      <strong>{statusType}</strong>
                      {statusMessage || query.status.status}

                      {seeResult}

                      <Link to='query' query={{handle: query.queryHandle.handleId}}
                        className='pull-right'>
                        Edit Query
                      </Link>

                    </p>
                </div>
              </div>
            </div>
          )}
        </div>
      </section>
    );
  }

  toggleQueryDetails () {
    this.setState({ showDetail: !this.state.showDetail });
  }

  cancelQuery () {
    let secretToken = UserStore.getUserDetails().secretToken;
    let handle = this.props && this.props.queryHandle &&
      this.props.queryHandle.handleId;

    if (!handle) return;

    AdhocQueryActions.cancelQuery(secretToken, handle);
  }
}

QueryPreview.propTypes = {
  queryHandle: React.PropTypes.string
};

export default QueryPreview;
