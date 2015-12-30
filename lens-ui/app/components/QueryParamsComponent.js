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
    this.state = {
      paramChanges: [],
      runImmediately: false,
      description: props.description
    };

    this.close = this.close.bind(this);
    this.save = this.save.bind(this);
    this.handleChange = this.handleChange.bind(this);
    this.handleCheck = this.handleCheck.bind(this);
    this.saveParamChanges = this.saveParamChanges.bind(this);
  }

  componentWillReceiveProps (props) {
    this.setState({description: props.description, paramChanges: []});
  }

  render () {
    let propParams = this.props.params;
    if (!propParams) return null;

    let changedParams = this.state.paramChanges;
    let params = this.mergeParamChanges(propParams, changedParams)
      .map(param => {
        return (
          <QueryParamRow key={param.name} param={param}
            saveParamChanges={this.saveParamChanges} />
        );
      });

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
            onChange={this.handleChange} id='queryDescription' value={this.state.description}
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
    // merges the initial props and the delta changes done by child components
    var parameters = this.mergeParamChanges(this.props.params, this.state.paramChanges);
    this.props.saveParams({
      parameters: parameters,
      description: this.state.description,
      runImmediately: this.state.runImmediately
    });
  }

  handleChange (e) {
    this.setState({ description: e.target.value });
  }

  handleCheck (e) {
    this.setState({ runImmediately: e.target.checked });
  }

  mergeParamChanges (original = [], changes = []) {
    return original.map(originalParam => {
      let change = changes.filter(changedParam => {
        return changedParam.name === originalParam.name;
      });
      return _.assign({}, originalParam, change[0]);
    });
  }

  saveParamChanges (changedParam) {
    // getting the param from the paramChanges state.
    var param = this.state.paramChanges.filter(param => {
      return param.name === changedParam.name;
    })[0];

    // apply the changedParam over the above param.
    var newParam = _.assign({}, param, changedParam);

    // getting all the other changes except the current as
    // we want to over-write it
    var newChangedParams = this.state.paramChanges.filter(param => {
      return param.name !== newParam.name;
    });

    this.setState({paramChanges: [...newChangedParams, newParam]});
  }
}

QueryParams.propTypes = {
  params: React.PropTypes.array.isRequired,
  close: React.PropTypes.func.isRequired,
  saveParams: React.PropTypes.func.isRequired
};

export default QueryParams;
