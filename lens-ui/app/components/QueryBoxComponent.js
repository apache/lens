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
import CodeMirror from 'codemirror';
import 'codemirror/lib/codemirror.css';
import 'codemirror/addon/edit/matchbrackets.js';
import 'codemirror/addon/hint/sql-hint.js';
import 'codemirror/addon/hint/show-hint.css';
import 'codemirror/addon/hint/show-hint.js';
import 'codemirror/mode/sql/sql.js';

import UserStore from '../stores/UserStore';
import AdhocQueryActions from '../actions/AdhocQueryActions';
import AdhocQueryStore from '../stores/AdhocQueryStore';
import CubeStore from '../stores/CubeStore';
import TableStore from '../stores/TableStore';
import DatabaseStore from '../stores/DatabaseStore';
import Config from 'config.json';
import '../styles/css/query-component.css';

// keeping a handle to CodeMirror instance,
// to be used to retrieve the contents of the editor
let codeMirror = null;

function setLimit (query) {
  // since pagination is not enabled on server, limit the query to 1000
  // check if user has specified existing limit > 1000, change it to 1000
  // dumb way, checking only last two words for `limit <number>` pattern
  let temp = query.split(' ');
  if (temp.slice(-2)[0].toLowerCase() === 'limit') {

    if (temp.slice(-1)[0] > 1000)  temp.splice(-1, 1, 1000);
    query = temp.join(' ');
  } else {
    query += ' limit 1000';
  }

  return query;
}

function setCode (code) {
  if (codeMirror) {
    codeMirror.setValue(code);
    codeMirror.focus();
  }
}

// used to populate the query box when user wants to edit a query
// TODO improve this.
// this takes in the query handle and writes the query
// used from Edit Query link
function fetchQuery (props) {
  if (props.query && props.query.handle) {
    let query = AdhocQueryStore.getQueries()[props.query.handle];

    if (query) {
      setCode(query.userQuery);
    }
  }
}

function setupCodeMirror (domNode) {

  // instantiating CodeMirror intance with some properties.
  codeMirror = CodeMirror.fromTextArea(domNode, {
    mode: 'text/x-sql',
    indentWithTabs: true,
    smartIndent: true,
    lineNumbers: true,
    matchBrackets : true,
    autofocus: true,
    lineWrapping: true
  });
}

function updateAutoComplete () {

  // add autocomplete hints to the query box
  let hints = {};

  // cubes
  let cubes = CubeStore.getCubes(); // hashmap
  Object.keys(cubes).forEach((cubeName) => {
    let cube = cubes[cubeName];
    hints[cubeName] = [];

    if (cube.measures && cube.measures.length) {
      cube.measures.forEach((measure) => {
        hints[cubeName].push(measure.name);
      });
    }
    if (cube.dimensions && cube.dimensions.length) {
      cube.dimensions.forEach((dimension) => {
        hints[cubeName].push(dimension.name);
      });
    }
  });

  //  tables
  let databases = DatabaseStore.getDatabases() || [];
  let tables = databases.map(db => {
    if (TableStore.getTables(db)) {
      return {
        database: db,
        tables: TableStore.getTables(db)
      }
    }
  }).filter(item => { return !!item; }); // filtering undefined items

  tables.forEach(tableObject => {
    Object.keys(tableObject.tables).forEach(tableName => {
      let table = tableObject.tables[tableName];
      let qualifiedName = tableObject.database + '.' + tableName;
      hints[qualifiedName] = [];
      hints[tableName] = [];

      if (table.columns && table.columns.length) {
        table.columns.forEach((col) => {
          hints[qualifiedName].push(col.name);
          hints[tableName].push(col.name);
          hints[col.name] = [];
        });
      }
    });
  });

  codeMirror.options.hintOptions = { tables: hints };
}

class QueryBox extends React.Component {
  constructor (props) {
    super(props);
    this.runQuery = this.runQuery.bind(this);
    this._onChange = this._onChange.bind(this);

    this.state = { querySubmitted: false, isRunQueryDisabled: true };
  }

  componentDidMount () {

    var editor = this.refs.queryEditor.getDOMNode();
    setupCodeMirror(editor);

    // disable 'Run Query' button when editor is empty
    // TODO: debounce this, as it'll happen on every key press. :(
    codeMirror.on('change', () => {
      codeMirror.getValue() ?
        this.state.isRunQueryDisabled = false :
        this.state.isRunQueryDisabled = true;

      this._onChange();
    });

    // to remove the previous query's submission notification
    codeMirror.on('focus', () => {
      this.state.querySubmitted = false;
    });

    // add Cmd + Enter to fire runQuery
    codeMirror.setOption("extraKeys", {
    'Cmd-Enter': (instance) => {
      this.runQuery();
    },
    'Ctrl-Space': 'autocomplete'
  });

    AdhocQueryStore.addChangeListener(this._onChange);
    CubeStore.addChangeListener(this._onChange);
    TableStore.addChangeListener(this._onChange);
  }

  componentWillUnmount () {
    AdhocQueryStore.addChangeListener(this._onChange);
    CubeStore.addChangeListener(this._onChange);
    TableStore.addChangeListener(this._onChange);
  }

  componentWillReceiveProps (props) {
    fetchQuery(props);
  }

  render () {
    let queryBoxClass = this.props.toggleQueryBox ? '': 'hide';

    return (
      <section className={queryBoxClass}>
        <div style={{borderBottom: '1px solid #dddddd'}}>
          <textarea ref="queryEditor"></textarea>
        </div>

        <div className="row" style={{padding: '6px 8px '}}>
          <div className="col-lg-4 col-md-4 col-sm-4 col-xs-12">
            <input type="text" className="form-control"
              placeholder="Query Name (optional)" ref="queryName"/>
          </div>
          <div className="col-lg-6 col-md-6 col-sm-6 col-xs-12">
            {this.state.querySubmitted && (
              <div className="alert alert-info" style={{padding: '5px 4px',
                marginBottom: '0px'}}>
                Query has been submitted. Results are on their way!
              </div>
            )}
          </div>
          <div className="col-lg-2 col-md-2 col-sm-2 col-xs-12">
            <button className="btn btn-primary responsive"
              onClick={this.runQuery} disabled={this.state.isRunQueryDisabled}>
              Run Query
            </button>
          </div>
        </div>
      </section>
    );
  }

  runQuery () {
    let queryName = this.refs.queryName.getDOMNode().value;
    let secretToken = UserStore.getUserDetails().secretToken;
    let query = codeMirror.getValue();

    // set limit if mode is in-memory
    if (!Config.isPersistent) query = setLimit(query);

    AdhocQueryActions.executeQuery(secretToken, query, queryName);

    // show user the query was posted successfully and empty the queryName
    this.state.querySubmitted = true;
    this.refs.queryName.getDOMNode().value = '';
  }

  _onChange () {

    // renders the detail result component if server
    // replied with a query handle.
    // this should ideally happen only when the 'Run Query' button is
    // clicked, and its action updates the store with query-handle.
    let handle = AdhocQueryStore.getQueryHandle();
    if (handle) {

      // clear it else detail result component will be rendered
      // every time the store emits a change event.
      AdhocQueryStore.clearQueryHandle();

      var { router } = this.context;
      router.transitionTo('result', {handle: handle});
    }

    // TODO remove this.
    // check if handle was passed as query param, and if that
    // query was fetched and available in store now.
    // if (this.props && this.props.query.handle) {
    //
    //   let query = AdhocQueryStore.getQueries()[this.props.query.handle];
    //   if (query)  setCode(query.userQuery);
    // }

    updateAutoComplete();
    this.setState(this.state);
  }
}

QueryBox.contextTypes = {
  router: React.PropTypes.func
};

export default QueryBox;
