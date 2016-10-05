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
import {Tooltip, OverlayTrigger} from 'react-bootstrap'

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
        <caption className='bg-primary text-left'>Measures</caption>
        <thead>{header}</thead>
        <tbody>{table}</tbody>
      </table>
    </div>
  );
}

function constructDimensionTable(cubeName, dimensions, join_chains_by_name) {
  let table = dimensions.map((dimension) => {
    if (typeof(dimension) == "string") {
      return (
        <tr key={cubeName + '|' + dimension}>
        </tr>
      );
    } else {
      let join_chain_column;
      if (dimension.chain_ref_column) {
        let inner =  dimension.chain_ref_column.map((ref) => {
          let join_chain = join_chains_by_name[ref.chain_name];
          let tooltip = <Tooltip id={dimension.name + "|" + ref.chain_name + "|tooltip"}><div> {
            join_chain.paths.path.map((path) => {
              let paths = path.edges.edge.map((edge) => {
                return edge.from.table + "." + edge.from.column + "=" + edge.to.table + "." + edge.to.column
              }).join("->")
              return (<span>{paths}<br/></span>)
            })
          }
          </div></Tooltip>;
          return (<div>
            <OverlayTrigger placement="top" overlay={tooltip}>
              <strong>{ref.chain_name}</strong>
            </OverlayTrigger>{"."+ref.ref_col}
          </div>);
        });
        join_chain_column = (<div>{inner}</div>);
      }
      return (
        <tr key={cubeName + '|' + dimension.name}>
          <td>{ dimension.name }</td>
          <td>{ dimension.display_string }</td>
          <td>{ dimension.description }</td>
          <td>{join_chain_column}</td>
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
        <caption className='bg-primary text-left'>Dim-Attributes</caption>
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
        <caption className='bg-primary text-left'>Join Chains</caption>
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
        <caption className='bg-primary text-left'>Expressions</caption>
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
    this.state = {cube: {}, database: UserStore.currentDatabase()};
    this._onChange = this._onChange.bind(this);

    AdhocQueryActions
      .getCubeDetails(UserStore.getUserDetails().secretToken, UserStore.currentDatabase(), props.params.cubeName);
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
    let cube = getCubes(UserStore.currentDatabase())[cubeName];

    if (cube.isLoaded) {
      this.setState({cube: cube, database: props.params.database});
      return;
    }

    AdhocQueryActions
      .getCubeDetails(UserStore.getUserDetails().secretToken, UserStore.currentDatabase(), cubeName);

    // empty the previous state
    this.setState({cube: {}, database: UserStore.currentDatabase()});
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
            { constructDimensionTable(cube.name, cube.dimensions, cube.join_chains_by_name) }
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
    let cube = getCubes(UserStore.currentDatabase())[this.props.params.cubeName];
    this.setState({cube: cube});
  }
}

CubeSchema.propTypes = {
  query: React.PropTypes.object,
  params: React.PropTypes.object
};

export default CubeSchema;
