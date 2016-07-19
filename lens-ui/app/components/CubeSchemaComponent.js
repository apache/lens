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

function getCubes(database) {
  return CubeStore.getCubes(database);
}

function constructMeasureTable(cubeName, measures) {
  let table = measures.map((measure) => {
    if (typeof(measure) == "string") {
      return (
        <tr key={cubeName + '|' + measure}>
          <td>{ measure }</td>
        </tr>
      );
    } else {
      return (
        <tr key={cubeName + '|' + measure.name}>
          <td>{ measure.name }</td>
          <td>{ measure.display_string }</td>
          <td>{ measure.description }</td>
        </tr>
      );
    }
  });

  let header = (
    <tr>
      <th>Name</th>
    </tr>
  );
  if (typeof(measures[0]) != "string") {
    header = (
      <tr>
        <th>Name</th>
        <th>Display String</th>
        <th>Description</th>
      </tr>
    );
  }

  return (
    <div className='table-responsive'>
      <table className='table table-striped table-condensed'>
        <caption className='bg-primary text-center'>Measures</caption>
        <thead>{header}</thead>
        <tbody>{table}</tbody>
      </table>
    </div>
  );
}

function constructDimensionTable(cubeName, dimensions) {
  let table = dimensions.map((dimension) => {
    if (typeof(dimension) == "string") {
      return (
        <tr key={cubeName + '|' + dimension}>
          <td>{ dimension}</td>
        </tr>
      );
    } else {
      return (
        <tr key={cubeName + '|' + dimension.name}>
          <td>{ dimension.name }</td>
          <td>{ dimension.display_string }</td>
          <td>{ dimension.description }</td>
          <td>{ dimension.chain_ref_column ? dimension.chain_ref_column.map((ref) => {
            return ref.chain_name + "." + ref.ref_col
          }).join("  ") : ""}</td>
        </tr>
      );
    }
  });
  let header = (
    <tr>
      <th>Name</th>
    </tr>
  );
  if (typeof(dimensions[0]) != "string") {
    header = (
      <tr>
        <th>Name</th>
        <th>Display String</th>
        <th>Description</th>
        <th>Source</th>
      </tr>
    );
  }
  return (
    <div className='table-responsive'>
      <table className='table table-striped'>
        <caption className='bg-primary text-center'>Dim-Attributes</caption>
        <thead>{header}</thead>
        <tbody>{table}</tbody>
      </table>
    </div>
  );
}

function constructJoinChainTable(cubeName, join_chains) {
  let table = join_chains.map((join_chain) => {
    return (
      <tr key={cubeName + '|' + join_chain.name}>
        <td>{ join_chain.name }</td>
        <td>{ <pre> {
          join_chain.paths.path.map((path) => {
            return path.edges.edge.map((edge) => {
              return edge.from.table + "." + edge.from.column + "=" + edge.to.table + "." + edge.to.column
            }).join("->")
          }).join("\n ")
        }
          </pre>
        }</td>
      </tr>
    );
  });
  let header = (
    <tr>
      <th>Name</th>
      <th>Paths</th>
    </tr>
  );
  return (
    <div className='table-responsive'>
      <table className='table table-striped'>
        <caption className='bg-primary text-center'>Join Chains</caption>
        <thead>{header}</thead>
        <tbody>{table}</tbody>
      </table>
    </div>
  );
}

function constructExpressionTable(cubeName, expressions) {
  let table = expressions.map((expression) => {
    return (
      <tr key={cubeName + '|' + expression.name}>
        <td>{ expression.name }</td>
        <td>{ expression.display_string }</td>
        <td>{ expression.description }</td>
        <td>{ <pre> {
          expression.expr_spec.map((expr_spec) => {
            return expr_spec.expr;
          }).join("\n ")
        }
          </pre>
        }</td>
      </tr>
    );
  });
  let header = (
    <tr>
      <th>Name</th>
      <th>Display String</th>
      <th>Description</th>
      <th>Expressions</th>
    </tr>
  );
  return (
    <div className='table-responsive'>
      <table className='table table-striped'>
        <caption className='bg-primary text-center'>Join Chains</caption>
        <thead>{header}</thead>
        <tbody>{table}</tbody>
      </table>
    </div>
  );
}

// TODO add prop checking.
class CubeSchema extends React.Component {
  constructor(props) {
    super(props);
    this.state = {cube: {}, database: props.params.databaseName};
    this._onChange = this._onChange.bind(this);

    AdhocQueryActions
      .getCubeDetails(UserStore.getUserDetails().secretToken, props.params.databaseName, props.params.cubeName);
  }

  componentDidMount() {
    CubeStore.addChangeListener(this._onChange);
  }

  componentWillUnmount() {
    CubeStore.removeChangeListener(this._onChange);
  }

  componentWillReceiveProps(props) {
    // TODO are props updated automatically, unlike state?
    let cubeName = props.params.cubeName;
    let cube = getCubes(props.params.databaseName)[cubeName];

    if (cube.isLoaded) {
      this.setState({cube: cube, database: props.params.database});
      return;
    }

    AdhocQueryActions
      .getCubeDetails(UserStore.getUserDetails().secretToken, props.params.databaseName, cubeName);

    // empty the previous state
    this.setState({cube: {}, database: props.params.databaseName});
  }

  render() {
    let schemaSection;

    // this will be empty if it's the first time so show a loader
    if (!this.state.cube.isLoaded) {
      schemaSection = <Loader size='8px' margin='2px'/>;
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
            { cube.join_chains && constructJoinChainTable(cube.name, cube.join_chains) }
            { cube.expressions && constructExpressionTable(cube.name, cube.expressions) }
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

  _onChange() {
    this.setState({cube: getCubes(this.props.params.databaseName)[this.props.params.cubeName]});
  }
}

CubeSchema.propTypes = {
  query: React.PropTypes.object,
  params: React.PropTypes.object
};

export default CubeSchema;
