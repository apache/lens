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

import TableStore from '../stores/TableStore';
import UserStore from '../stores/UserStore';
import AdhocQueryActions from '../actions/AdhocQueryActions';
import Loader from './LoaderComponent';

function getTable (tableName, database) {
  let tables = TableStore.getTables(database);
  return tables && tables[tableName];
}

class TableSchema extends React.Component {
  constructor (props) {
    super(props);
    this.state = {table: {}};
    this._onChange = this._onChange.bind(this);

    let secretToken = UserStore.getUserDetails().secretToken;
    let tableName = props.params.tableName;
    let database = props.query.database;
    AdhocQueryActions.getTableDetails(secretToken, tableName, database);
  }

  componentDidMount () {
    TableStore.addChangeListener(this._onChange);
  }

  componentWillUnmount () {
    TableStore.removeChangeListener(this._onChange);
  }

  componentWillReceiveProps (props) {
    let tableName = props.params.tableName;
    let database = props.query.database;
    if (!TableStore.getTables(database)[tableName].isLoaded) {
      let secretToken = UserStore.getUserDetails().secretToken;

      AdhocQueryActions
        .getTableDetails(secretToken, tableName, database);

      // set empty state as we do not have the loaded data.
      this.setState({table: {}});
      return;
    }

    this.setState({
      table: TableStore.getTables(database)[tableName]
    });
  }

  render () {
    let schemaSection = null;

    if (this.state.table && !this.state.table.isLoaded) {
      schemaSection = <Loader size='8px' margin='2px' />;
    } else {
      schemaSection = (<div className='row'>
          <div className='table-responsive'>
            <table className='table table-striped'>
              <thead>
              <caption className='bg-primary text-center'>Columns</caption>
                <tr><th>Name</th><th>Type</th><th>Description</th></tr>
              </thead>
              <tbody>
                {this.state.table &&
                  this.state.table.columns.map(col => {
                    return (
                      <tr key={this.state.table.name + '|' + col.name}>
                        <td>{col.name}</td>
                        <td>{col._type}</td>
                        <td>{col.comment || 'No description available'}</td>
                      </tr>
                    );
                  })
                }
              </tbody>
            </table>
          </div>
        </div>);
    }

    return (
      <section>
        <div className='panel panel-default'>
          <div className='panel-heading'>
            <h3 className='panel-title'>Schema Details: &nbsp;
              <strong className='text-primary'>
                 {this.props.query.database}.{this.props.params.tableName}
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
    this.setState({
      table: getTable(this.props.params.tableName,
        this.props.query.database)
    });
  }
}

TableSchema.propTypes = {
  query: React.PropTypes.object,
  params: React.PropTypes.object
};

export default TableSchema;
