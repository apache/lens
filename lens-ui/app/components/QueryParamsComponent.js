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
import { Button, Input } from 'react-bootstrap';
import _ from 'lodash';

import QueryParamRow from './QueryParamRowComponent';

class QueryParams extends React.Component {
  constructor (props) {
    super(props);
    this.state = {description: '', childrenParams: {}, runImmediately: false};

    this.close = this.close.bind(this);
    this.save = this.save.bind(this);
    this.update = this.update.bind(this);
    this.handleChange = this.handleChange.bind(this);
    this.handleCheck = this.handleCheck.bind(this);
    this._getChildrenParams = this._getChildrenParams.bind(this);
  }

  componentWillReceiveProps (props) {
    if (!_.isEqual(props.params, this.props.params)) {
      this.state.childrenParams = {};
    }
  }

  render () {
    let params = this.props.params && this.props.params.map((param, index) => {
      return <QueryParamRow key={param.name} param={param} updateParam={this.update}/>;
    });

    if (!params) return null;

    return (
      <form onSubmit={this.save} style={{padding: '10px', boxShadow: '2px 2px 2px 2px grey',
        marginTop: '6px', backgroundColor: 'rgba(255, 255, 0, 0.1)'}}>
        <h3>
          Query Parameters
        </h3>
        <table className='table table-striped'>
          <thead>
            <tr>
              <th>Parameter</th>
              <th>Display Name</th>
              <th>Data Type</th>
              <th>Collection Type</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {params}
          </tbody>
        </table>
        <div className='form-group'>
          <label className='sr-only' htmlFor='queryDescription'>Description</label>
          <input type='text' className='form-control' style={{fontWeight: 'normal'}}
            onChange={this.handleChange} id='queryDescription'
            placeholder='(Optional description) e.g. This awesome query does magic along with its job.'
          />
        </div>
        <div>
            <Input type='checkbox' label='Run after saving'
              onChange={this.handleCheck} />

        </div>
        <Button bsStyle='primary' type='submit'>Save</Button>
        <Button onClick={this.close} style={{marginLeft: '4px'}}>Cancel</Button>
      </form>
    );
  }

  close () {
    this.props.close();
  }

  save (e) {
    e.preventDefault();
    var parameters = this._getChildrenParams();
    this.props.saveParams({
      parameters: parameters,
      description: this.state.description,
      runImmediately: this.state.runImmediately
    });
  }

  _getChildrenParams () {
    return Object.keys(this.state.childrenParams).map(name => {
      return this.state.childrenParams[name];
    });
  }

  handleChange (e) {
    this.setState({description: e.target.value});
  }

  handleCheck (e) {
    this.setState({runImmediately: e.target.checked});
  }

  // called by the child component {name, param}
  update (param) {
    this.state.childrenParams[param.name] = param.param;
  }
}

QueryParams.propTypes = {
  params: React.PropTypes.array.isRequired,
  close: React.PropTypes.func.isRequired,
  saveParams: React.PropTypes.func.isRequired
};

export default QueryParams;
