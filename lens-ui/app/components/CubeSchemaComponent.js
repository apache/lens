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

import CubeStore from '../stores/CubeStore';
import UserStore from '../stores/UserStore';
import AdhocQueryActions from '../actions/AdhocQueryActions';
import Loader from '../components/LoaderComponent';

function getCubes () {
  return CubeStore.getCubes();
}

function constructMeasureTable (cubeName, measures) {
  let table = measures.map((measure) => {
    return (
      <tr key={cubeName + '|' + measure.name}>
        <td>{ measure.name }</td>
        <td>{ measure.type }</td>
        <td>{ measure.default_aggr }</td>
        <td>{ measure.display_string }</td>
      </tr>
    );
  });

  return (
    <div className='table-responsive'>
      <table className='table table-striped table-condensed'>
        <caption className='bg-primary text-center'>Measures</caption>
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Default Aggr</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          {table}
        </tbody>
      </table>
    </div>
  );
}

function constructDimensionTable (cubeName, dimensions) {
  let table = dimensions.map((dimension) => {
    return (
      <tr key={cubeName + '|' + dimension.name}>
        <td>{ dimension.name }</td>
        <td>{ dimension.type }</td>
        <td>{ dimension.ref_spec && dimension.ref_spec.chain_ref_column &&
          dimension.ref_spec.chain_ref_column.dest_table }</td>
        <td>{ dimension.ref_spec && dimension.ref_spec.chain_ref_column &&
          dimension.ref_spec.chain_ref_column.ref_col }</td>
        <td>{ dimension.description }</td>
      </tr>
    );
  });

  return (
    <div className='table-responsive'>
      <table className='table table-striped'>
        <caption className='bg-primary text-center'>Dimensions</caption>
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Destination Table</th>
            <th>Column</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          {table}
        </tbody>
      </table>
    </div>
  );
}

// TODO add prop checking.
class CubeSchema extends React.Component {
  constructor (props) {
    super(props);
    this.state = {cube: {}};
    this._onChange = this._onChange.bind(this);

    // firing the action for the first time component is rendered
    // it won't have a cube in the state.
    let cubeName = props.params.cubeName;
    AdhocQueryActions
      .getCubeDetails(UserStore.getUserDetails().secretToken, cubeName);
  }

  componentDidMount () {
    CubeStore.addChangeListener(this._onChange);
  }

  componentWillUnmount () {
    CubeStore.removeChangeListener(this._onChange);
  }

  componentWillReceiveProps (props) {
    // TODO are props updated automatically, unlike state?
    let cubeName = props.params.cubeName;
    let cube = getCubes()[cubeName];

    if (cube.isLoaded) {
      this.setState({ cube: getCubes()[cubeName] });
      return;
    }

    AdhocQueryActions
      .getCubeDetails(UserStore.getUserDetails().secretToken, cubeName);

    // empty the previous state
    this.setState({ cube: {} });
  }

  render () {
    let schemaSection;

    // this will be empty if it's the first time so show a loader
    if (!this.state.cube.isLoaded) {
      schemaSection = <Loader size='8px' margin='2px' />;
    } else {
      // if we have cube state
      let cube = this.state.cube;
      if (this.props.query.type === 'measures') {
        // show only measures
        schemaSection = constructMeasureTable(cube.name, cube.measures);
      } else if (this.props.query.type === 'dimensions') {
        // show only dimensions
        schemaSection = constructDimensionTable(cube.name, cube.dimensions);
      } else {
        // show both measures, dimensions
        schemaSection = (
          <div>
            { constructMeasureTable(cube.name, cube.measures) }
            { constructDimensionTable(cube.name, cube.dimensions) }
          </div>
        );
      }
    }

    // TODO give max height to panel-body depending upon
    // whether the query box is visible or not.
    return (

      <section>
        <div className='panel panel-default'>
          <div className='panel-heading'>
            <h3 className='panel-title'>Schema Details: &nbsp;
              <strong className='text-primary'>
                 {this.props.params.cubeName}
              </strong>
            </h3>
          </div>
          <div className='panel-body'>
            {schemaSection}
          </div>
        </div>

      </section>
    );
  }

  _onChange () {
    this.setState({cube: getCubes()[this.props.params.cubeName]});
  }
}

CubeSchema.propTypes = {
  query: React.PropTypes.object,
  params: React.PropTypes.object
};

export default CubeSchema;
