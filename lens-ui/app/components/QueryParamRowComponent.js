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
import { Multiselect } from 'react-widgets';
import assign from 'object-assign';
import _ from 'lodash';
import 'react-widgets/dist/css/core.css';
import 'react-widgets/dist/css/react-widgets.css';

class QueryParamRow extends React.Component {
  constructor (props) {
    super(props);

    this.state = {
      paramChange: assign({}, props.param)
    };

    this._handleChange = this._handleChange.bind(this);
    this.getDefaultValueInput = this.getDefaultValueInput.bind(this);
  }

  shouldComponentUpdate (newProps, newState) {
    return !_.isEqual(this.state, newState);
  }

  render () {
    let param = this.props.param;

    let collectionType = this.state.paramChange.collectionType;
    let dataType = this.state.paramChange.dataType;

    let collectionTypeBox = (<select className='form-control' required
      defaultValue='SINGLE' onChange={this._handleChange('ChangeCollectionType')}>
        <option value='SINGLE'>Single</option>
        <option value='MULTIPLE'>Multiple</option>
      </select>);

    let valueBox = this.getDefaultValueInput(collectionType, dataType);

    return (
      <tr>
        <td>{param.name}</td>
        <td>
          { this.props.entryMode ? param.displayName :
            <input type='text' className='form-control' required defaultValue={param.name}
              placeholder='display name' onChange={this._handleChange('ChangeDisplayName')}/>
          }
        </td>
        <td>
          { this.props.entryMode ? param.dataType :
            <select className='form-control' defaultValue={dataType || 'STRING'}
              onChange={this._handleChange('ChangeDataType')}>
              <option value='STRING'>String</option>
              <option value='NUMBER'>Number</option>
              <option value='BOOLEAN'>Boolean</option>
            </select>
          }
        </td>
        <td>
          { this.props.entryMode ? param.collectionType :
            {collectionTypeBox}
          }

        </td>
        <td>
          {valueBox}
        </td>
      </tr>
    );
  }

  _handleChange (elementType) {
    return (arg) => {
      let paramChange;
      let state = _.assign({}, this.state.paramChange);
      let val;
      switch (elementType) {
        case 'ChangeMultiselect':
          paramChange = _.assign({}, state, {defaultValue: arg});
          break;

        case 'ChangeDefaultTextValue':
          paramChange = _.assign({}, state, {defaultValue: arg.target.value});
          break;

        case 'AddItemInMultiSelect':
          this.state.paramChange.defaultValue.push(arg);
          paramChange = _.assign({}, this.state.paramChange, {
            //defaultValue: [...this.state.paramChange.defaultValue, item]
          });
          break;

        case 'ChangeDataType':
          val = this.state.paramChange.collectionType === 'SINGLE' ? null : [];
          paramChange = _.assign({}, state, {
            dataType: arg.target.value,
            defaultValue: val,
          });
          break;

        case 'ChangeCollectionType':
          val = arg.target.value === 'MULTIPLE' ? [] : null;
          paramChange = _.assign({}, state, {
            collectionType: arg.target.value,
            defaultValue: val
          });
          break;

        case 'ChangeDisplayName':
          paramChange = _.assign({}, state, {displayName: arg.target.value});
          break;
      }

      this.setState({paramChange});
      this.props.saveParamChanges(paramChange);
    };
  }


  preventEnter (e) {
    if (e.keyCode == 13) e.preventDefault();
  }

  getDefaultValueInput (collectionType, dataType) {
    let valueBox = null;
    if (collectionType === 'SINGLE') {
      valueBox = <input type='text' className='form-control' required value={this.state.paramChange.defaultValue}
        placeholder='default value' onChange={this._handleChange('ChangeDefaultTextValue')}/>;
    } else if (collectionType === 'MULTIPLE') {
      valueBox = <Multiselect messages={{createNew: 'Enter to add'}}
         onCreate={this._handleChange('AddItemInMultiSelect')} data={this.state.paramChange.defaultValue}
         onChange={this._handleChange('ChangeMultiselect')}
        value={this.state.paramChange.defaultValue} onKeyDown={this.preventEnter}
      />;
    }
    return valueBox;
  }
}

QueryParamRow.propTypes = {
  param: React.PropTypes.object.isRequired,
  saveParamChanges: React.PropTypes.func.isRequired,
  entryMode: React.PropTypes.boolean
};

export default QueryParamRow;
