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
import TreeView from 'react-treeview';
import assign from 'object-assign';
import { Link } from 'react-router';
import 'react-treeview/react-treeview.css';
import ClassNames from 'classnames';

import DatabaseStore from '../stores/DatabaseStore';
import CubeStore from '../stores/CubeStore';
import AdhocQueryActions from '../actions/AdhocQueryActions';
import UserStore from '../stores/UserStore';
import Loader from '../components/LoaderComponent';
import '../styles/css/tree.css';

function getCubeData () {
  return {
    cubes: CubeStore.getCubes(UserStore.currentDatabase())
  };
}

class CubeTree extends React.Component {
  constructor (props) {
    super(props);

    // setting the initial state, as getInitialState only
    // comes with React.createClass, using constructor is the new
    // idiomatic way
    // https://facebook.github.io/react/blog/2015/01/27/react-v0.13.0-beta-1.html
    this.state = {cubes: CubeStore.getCubes(UserStore.currentDatabase()), loading: false, isCollapsed: false };

    // no autobinding with ES6 so doing it manually, see link below
    // https://facebook.github.io/react/blog/2015/01/27/react-v0.13.0-beta-1.html#autobinding
    this._onChange = this._onChange.bind(this);
    this.toggle = this.toggle.bind(this);
    if (!this.state.cubes) {
      this.state.cubes = this.state.cubes || [];
      this.state.loading = true;
      AdhocQueryActions.getCubes(UserStore.getUserDetails().secretToken, UserStore.currentDatabase());
    }
  }

  componentDidMount () {
    CubeStore.addChangeListener(this._onChange);
  }

  componentWillUnmount () {
    CubeStore.removeChangeListener(this._onChange);
  }

  render () {
    // cube tree structure sample
    // [{
    //   name: 'Cube-1',
    //   measures: [{name: 'Measure-1'}, {name: 'Measure-1'}], // optional
    //   dimensions: [{name: 'Dimension-1'}, {name: 'Dimension-1'}] //optional
    // }, ...]

    var cubeHash = assign({}, this.state.cubes);
    var cubeTree = Object.keys(this.state.cubes).map((cubeName, i) => {
      let cube = cubeHash[cubeName];

      let label = <Link to='cubeschema' params={{databaseName: this.state.database, cubeName: cubeName}}>
          <span className='node'>{cube.name}</span>
        </Link>;

      let measureLabel = <Link to='cubeschema' params={{databaseName: this.state.database, cubeName: cubeName}}
        query={{type: 'measures'}}>
          <span className='quiet'>Measures</span>
        </Link>;

      let dimensionLabel = <Link to='cubeschema' params={{databaseName: this.state.database, cubeName: cubeName}}
        query={{type: 'dimensions'}}>
          <span className='quiet'>Dim-Attributes</span>
        </Link>;
      return (
        <TreeView key={cube.name + '|' + i} nodeLabel={label}
          defaultCollapsed={!cube.isLoaded} onClick={this.getDetails.bind(this, cube.name)}>

          <TreeView key={cube.name + '|measures'} nodeLabel={measureLabel}
            defaultCollapsed={!cube.isLoaded}>
            { cube.measures ? cube.measures.map(measure => {
              return (
                <div key={measure.name} className='treeNode measureNode'>
                  {measure.name} ({measure.default_aggr})
                </div>
              );
            }) : null }
          </TreeView >

          <TreeView key={cube.name + '|dim attributes'} nodeLabel={dimensionLabel}
            defaultCollapsed={!cube.isLoaded}>
            { cube.dimensions ? cube.dimensions.map(dimension => {
              return (
                <div className='treeNode dimensionNode' key={dimension.name}>
                  {dimension.name}
                </div>
              );
            }) : null }
          </TreeView >

        </TreeView >
      );
    });

    if (this.state.loading) {
      cubeTree = <Loader size='4px' margin='2px'/>;
    } else if (!Object.keys(this.state.cubes).length) {
      cubeTree = (<div className='alert-danger' style={{padding: '8px 5px'}}>
          <strong>Sorry, we couldn&#39;t find any cubes.</strong>
        </div>);
    }

    let collapseClass = ClassNames({
      'pull-right': true,
      'glyphicon': true,
      'glyphicon-chevron-up': !this.state.isCollapsed,
      'glyphicon-chevron-down': this.state.isCollapsed
    });

    let panelBodyClassName = ClassNames({
      'panel-body': true,
      'hide': this.state.isCollapsed
    });

    return (
      <div className='panel panel-default'>
        <div className='panel-heading'>
          <h3 className='panel-title'>
            Cubes
            <span className={collapseClass} onClick={this.toggle}></span>
          </h3>
        </div>
        <div className={panelBodyClassName} style={{maxHeight: '350px', overflowY: 'auto'}}>
          {cubeTree}
        </div>
      </div>
    );
  }

  _onChange () {
    let state = getCubeData();
    state.loading = false;
    this.setState(state);
  }

  // TODO: check its binding in the onClick method
  // needs to be investigated
  // https://facebook.github.io/react/tips/communicate-between-components.html
  getDetails (cubeName) {
    if (this.state.cubes[cubeName].isLoaded) return;

    AdhocQueryActions
      .getCubeDetails(UserStore.getUserDetails().secretToken, cubeName);
  }

  toggle () {
    this.setState({isCollapsed: !this.state.isCollapsed});
  }
}

export default CubeTree;
