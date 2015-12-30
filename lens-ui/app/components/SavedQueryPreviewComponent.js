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
import CodeMirror from 'codemirror';
import _ from 'lodash';
import 'codemirror/mode/sql/sql.js';
import 'codemirror/addon/runmode/runmode.js';

import QueryParamRowComponent from './QueryParamRowComponent';
import AdhocQueryActions from '../actions/AdhocQueryActions';
import UserStore from '../stores/UserStore';

class SavedQueryPreview extends React.Component {
  constructor (props) {
    super(props);
    this.state = {
      showDetail: false,
      queryParams: props.query.parameters.reduce((prev, curr) => {
        prev[curr.name] = curr;
        return prev;
      }, {})
    };
    this.toggleQueryDetails = this.toggleQueryDetails.bind(this);
    this.runSavedQuery = this.runSavedQuery.bind(this);
    this.update = this.update.bind(this);
  }

  render () {
    let query = this.props.query;

    if (!query.query) return null;

    // code below before the return prepares the data to render, turning out
    // crude properties to glazed, garnished ones e.g. formatting of query
    let codeTokens = [];

    CodeMirror
      .runMode(query.query,
        'text/x-mysql', function (text, style) {
        // this method is called for every token and gives the
        // token and style class for it.
        codeTokens.push(<span className={'cm-' + style}>{text}</span>);
      });

    let params = query && query.parameters.map(param => {
      return <QueryParamRowComponent param={param} entryMode={true}
        saveParamChanges={this.update}/>;
    });

    let paramsTable = !params.length ? null :
      (<table className='table table-striped table-condensed'>
        <thead>
          <tr><th>Param</th><th>Display Name</th><th>Data Type</th>
          <th>Collection Type</th><th>Value</th></tr>
        </thead>
        <tbody>
          {params}
        </tbody>
      </table>);

    return (
      <section ref='preview'>
        <div className='panel panel-default'>
          <div className='panel-heading blue-header' style={{cursor: 'pointer', padding: '2px 8px'}}
            onClick={this.toggleQueryDetails}>
            <h5 className='panel-title' style={{marginBottom: '4px'}}>
              {query.name || 'Unnamed Query'}
            </h5>
            <small className='italics'>
              { query.description || 'No description available' }
            </small>
          </div>

          {this.state.showDetail && (
            <div className='panel-body' style={{borderTop: '1px solid #cccccc',
              padding: '0px'}} key={'preview' + query.id}>
              <pre className='cm-s-default' style={{
                border: '0px', marginBottom: '0px'}}>
                {codeTokens}

                <Link to='query' query={{savedquery: query.id}}
                  className='btn btn-default pull-right'>
                  <i className='fa fa-pencil fa-lg'></i>
                </Link>
                <button className='btn btn-default pull-right' onClick={this.runSavedQuery}>
                  <i className='fa fa-play fa-lg'></i>
                </button>
              </pre>

              {paramsTable}
            </div>
          )}
        </div>
      </section>
    );
  }

  toggleQueryDetails () {
    this.setState({ showDetail: !this.state.showDetail });
  }

  update (param) {
    this.setState({
      queryParams: _.assign({}, this.state.queryParams, {[param.name]: param})
    });
  }

  runSavedQuery () {
    let secretToken = UserStore.getUserDetails().secretToken;
    let parameters = Object.keys(this.state.queryParams).map(paramName => {
      return {
        [paramName]: this.state.queryParams[paramName].defaultValue
      };
    });
    AdhocQueryActions.runSavedQuery(secretToken, this.props.query.id, parameters);
  }
}

SavedQueryPreview.propTypes = {
  query: React.PropTypes.object,
  params: React.PropTypes.object
};

export default SavedQueryPreview;
