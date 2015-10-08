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
import 'react-widgets/dist/css/core.css';
import 'react-widgets/dist/css/react-widgets.css';

// returns true/false if the default value is correct
// and also returns the value
function validate (val, dataType) {
  // if (dataType === 'NUMBER' && !window.isNaN(val)) return [true, val];
  // if (dataType === 'BOOLEAN' && (val === 'true' || val === 'false')) {
  //   return [true, val];
  // }
  // if (dataType === 'STRING' && typeof val === 'string') return [true, val];

  return [true, val];
}

class QueryParamRow extends React.Component {
  constructor (props) {
    super(props);

    // state being decided by mode of use of this component
    // `entryMode` is used by the SavedQueryPreviewComponent,
    // to just add values and run the saved query.
    if (props.entryMode) {
      this.state = assign({}, props.param);
    } else {
      this.state = assign({}, props.param, {
        dataType: 'STRING',
        collectionType: 'SINGLE',
        displayName: props.param.name
      });
    }

    this.changeDisplayName = this.changeDisplayName.bind(this);
    this.changeDataType = this.changeDataType.bind(this);
    this.changeCollectionType = this.changeCollectionType.bind(this);
    this.changeDefaultValue = this.changeDefaultValue.bind(this);
    this.addDefaultValue = this.addDefaultValue.bind(this);
    this.preventEnter = this.preventEnter.bind(this);
  }

  componentWillReceiveProps (props) {
    this.setState(assign({}, props.param));
  }

  componentWillUpdate (props, state) {
    this.props.updateParam({
      name: props.param.name,
      param: state
    });
  }

  render () {
    let param = this.props.param;

    return (
      <tr>
        <td>{param.name}</td>
        <td>
          { this.props.entryMode ? param.displayName :
            <input type='text' className='form-control' required defaultValue={param.name}
              placeholder='display name' onChange={this.changeDisplayName}/>
          }
        </td>
        <td>
          { this.props.entryMode ? param.dataType :
            <select className='form-control' defaultValue='STRING'
              onChange={this.changeDataType}>
              <option value='STRING'>String</option>
              <option value='NUMBER'>Number</option>
              <option value='BOOLEAN'>Boolean</option>
            </select>
          }
        </td>
        <td>
          { this.props.entryMode ? param.collectionType :
            <select className='form-control' required defaultValue='SINGLE'
              onChange={this.changeCollectionType}>
              <option value='SINGLE'>Single</option>
              <option value='MULTIPLE'>Multiple</option>
            </select>
          }

        </td>
        <td>
          { !this.props.entryMode && (this.state.collectionType === 'SINGLE' ?
            <input type='text' className='form-control' required value={this.state.defaultValue}
              placeholder='default value' onChange={this.changeDefaultValue}/> :
            <Multiselect messages={{createNew: 'Enter to add'}}
              onCreate={this.addDefaultValue}
              defaultValue={this.state.defaultValue} onKeyDown={this.preventEnter}
            />
          )}

          { this.props.entryMode && (param.collectionType === 'SINGLE' ?
            <input type='text' className='form-control' required value={this.state.defaultValue}
              placeholder='default value' onChange={this.changeDefaultValue}/> :
            <Multiselect messages={{createNew: 'Enter to add'}}
               onCreate={this.addDefaultValue}
              defaultValue={this.state.defaultValue} onKeyDown={this.preventEnter}
            />
          )}
        </td>
      </tr>
    );
  }

  // these methods change the default values
  // called by normal input
  changeDefaultValue (e) {
    let val = validate(e.target.value, this.state.dataType);

    if (val[0]) this.setState({defaultValue: val[1]});
  }

  // called my multiselect
  addDefaultValue (item) {
    let val = validate(item, this.state.dataType);

    if (val[0]) {
      this.state.defaultValue.push(val[1]);
      this.setState(this.state);
    }
  }

  preventEnter (e) {
    if (e.keyCode == 13) e.preventDefault();
  }

  changeDataType (e) {
    let val = this.state.collectionType === 'SINGLE' ? null : [];
    this.setState({dataType: e.target.value, defaultValue: val});
  }

  changeCollectionType (e) {
    let val = e.target.value === 'MULTIPLE' ? [] : null;
    this.setState({defaultValue: val});
    this.setState({collectionType: e.target.value});
  }

  changeDisplayName (e) {
    this.setState({displayName: e.target.value});
  }
}

QueryParamRow.propTypes = {
  param: React.PropTypes.object.isRequired,
  updateParam: React.PropTypes.func.isRequired,
  entryMode: React.PropTypes.boolean
};

export default QueryParamRow;
