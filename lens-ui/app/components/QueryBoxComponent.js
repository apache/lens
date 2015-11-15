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
import ClassNames from 'classnames';
import CodeMirror from 'codemirror';
import assign from 'object-assign';
import _ from 'lodash';
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
import SavedQueryStore from '../stores/SavedQueryStore';
import QueryParams from './QueryParamsComponent';
import Config from 'config.json';
import '../styles/css/query-component.css';

// keeping a handle to CodeMirror instance,
// to be used to retrieve the contents of the editor
let codeMirror = null;
let codeMirrorHints = {};

// list of possible client messages
let clientMessages = {
  runQuery: 'Running your query...',
  saveQuery: 'Saving your query...',
  noName: 'Name is mandatory for a saved query.',
  updateQuery: 'Updating saved query...'
};

function setLimit (query) {
  // since pagination is not enabled on server, limit the query to 1000
  // check if user has specified existing limit > 1000, change it to 1000
  // dumb way, checking only last two words for `limit <number>` pattern
  let temp = query.split(' ');
  if (temp.slice(-2)[0].toLowerCase() === 'limit') {
    if (temp.slice(-1)[0] > 1000) temp.splice(-1, 1, 1000);
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

function getEmptyState () {
  return {
    clientMessage: null, // to give user instant ack
    isRunQueryDisabled: true,
    serverMessage: null, // type (success or error), text as keys
    isCollapsed: false,
    params: null,
    isModeEdit: false,
    savedQueryId: null,
    runImmediately: false,
    description: ''
  };
}

// used to populate the query box when user wants to edit a query
// TODO improve this.
// this takes in the query handle and writes the query
// used from Edit Query link
function fetchQueryForEdit (props) {
  let query = AdhocQueryStore.getQueries()[props.query.handle];

  if (query) {
    setCode(query.userQuery);
  }
}

function setupCodeMirror (domNode) {
  // instantiating CodeMirror intance with some properties.
  codeMirror = CodeMirror.fromTextArea(domNode, {
    mode: 'text/x-sql',
    indentWithTabs: true,
    smartIndent: true,
    lineNumbers: true,
    matchBrackets: true,
    autofocus: true,
    lineWrapping: true
  });
}

class QueryBox extends React.Component {
  constructor (props) {
    super(props);
    this.runQuery = this.runQuery.bind(this);
    this.saveQuery = this.saveQuery.bind(this);
    this._onChange = this._onChange.bind(this);
    this.toggle = this.toggle.bind(this);
    this.closeParamBox = this.closeParamBox.bind(this);
    this.saveParams = this.saveParams.bind(this);
    this._onChangeSavedQueryStore = this._onChangeSavedQueryStore.bind(this);
    this._getSavedQueryDetails = this._getSavedQueryDetails.bind(this);
    this.cancel = this.cancel.bind(this);
    this.saveOrUpdate = this.saveOrUpdate.bind(this);
    this.runSavedQuery = this.runSavedQuery.bind(this);

    this.state = getEmptyState();
  }

  componentDidMount () {
    var editor = this.refs.queryEditor.getDOMNode();
    setupCodeMirror(editor);

    // disable 'Run Query' button when editor is empty
    // TODO: debounce this, as it'll happen on every key press. :(
    codeMirror.on('change', () => {
      this.state.isRunQueryDisabled = !codeMirror.getValue();
      this.setState(this.state);

      this._onChange();
    });

    // to remove the previous query's submission notification
    codeMirror.on('focus', () => {
      this.setState({ clientMessage: null });
    });

    // add Cmd + Enter to fire runQuery
    codeMirror.setOption('extraKeys', {
      'Cmd-Enter': instance => { this.runQuery(); },
      'Ctrl-Space': 'autocomplete',
      'Ctrl-S': instance => { this.saveQuery(); }
    });

    AdhocQueryStore.addChangeListener(this._onChange);
    CubeStore.addChangeListener(this._onChangeCubeStore);
    TableStore.addChangeListener(this._onChangeTableStore);
    SavedQueryStore.addChangeListener(this._onChangeSavedQueryStore);
  }

  componentWillUnmount () {
    AdhocQueryStore.removeChangeListener(this._onChange);
    CubeStore.removeChangeListener(this._onChangeCubeStore);
    TableStore.removeChangeListener(this._onChangeTableStore);
    SavedQueryStore.removeChangeListener(this._onChangeSavedQueryStore);
  }

  componentWillReceiveProps (props) {
    // normal query
    if (props.query && props.query.handle) {
      fetchQueryForEdit(props);
      // clear saved query state
      this.setState({
        params: null,
        savedQueryId: null,
        isModeEdit: false
      });
    // saved query
    } else if (props.query && props.query.savedquery) {
      let queryId = props.query.savedquery;
      let savedQuery = SavedQueryStore.getSavedQueries()[queryId];
      if (savedQuery) {
        setCode(savedQuery.query);
        this.refs.queryName.getDOMNode().value = savedQuery.name;
        this.setState({
          params: savedQuery.parameters,
          savedQueryId: savedQuery.id,
          description: savedQuery.description,
          isModeEdit: true
        });
      }
    }
  }

  render () {
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

    let notificationClass = ClassNames({
      'alert': true,
      'alert-danger': this.state.serverMessage && this.state.serverMessage.type === 'Error',
      'alert-success': this.state.serverMessage && this.state.serverMessage.type !== 'Error'
    });

    return (

      <div className='panel panel-default'>
        <div className='panel-heading'>
          <h3 className='panel-title'>
            {this.state.isModeEdit ? 'Edit' : 'Compose'}
            <span className={collapseClass} onClick={this.toggle}></span>
          </h3>
        </div>
        <div className={panelBodyClassName} style={{padding: '0px'}}>
          <section>
            <div style={{borderBottom: '1px solid #dddddd'}}>
              <textarea ref='queryEditor'></textarea>
            </div>

            <div className='row' style={{padding: '6px 8px '}}>
              <div className='col-lg-4 col-md-4 col-sm-4 col-xs-12'>
                <input type='text' className='form-control'
                  placeholder='Query Name (optional)' ref='queryName'/>
              </div>
              <div className='col-lg-5 col-md-5 col-sm-5 col-xs-12'>
                {this.state.clientMessage && (
                  <div className='alert alert-info' style={{padding: '5px 4px',
                    marginBottom: '0px'}}>
                    {this.state.clientMessage}
                  </div>
                )}
              </div>
              <div className='col-lg-3 col-md-3 col-sm-3 col-xs-12'>
                <button className='btn btn-default' style={{marginRight: '4px'}}
                  onClick={this.saveOrUpdate} disabled={this.state.isRunQueryDisabled}
                  title='Save'>
                  <i className='fa fa-save fa-lg'></i>
                </button>
                <button className='btn btn-default' title='Run'
                  onClick={this.runQuery} style={{marginRight: '4px'}}
                  disabled={this.state.isRunQueryDisabled}>
                  <i className='fa fa-play fa-lg'></i>
                </button>
                <button className='btn btn-default' onClick={this.cancel}
                  title='Clear'>
                  <i className='fa fa-ban fa-lg'></i>
                </button>
              </div>
            </div>

            { this.state.params && !!this.state.params.length &&
              <QueryParams params={this.state.params} close={this.closeParamBox}
                saveParams={this.saveParams} description={this.state.description}/>
            }

            { this.state.serverMessage &&
              <div className={notificationClass} style={{marginBottom: '0px'}}>

                {this.state.serverMessage.text}

                { this.state.serverMessage.texts &&
                  this.state.serverMessage.texts.map(e => {
                    return (
                      <li style={{listStyleType: 'none'}}>
                        <strong>{e.code}</strong>: <span>{e.message}</span>
                      </li>
                    );
                  })
                }
              </div>
            }
          </section>
        </div>
      </div>
    );
  }

  saveOrUpdate () {
    !this.state.isModeEdit ? this.saveQuery() : this.updateQuery();
  }

  runQuery () {
    let queryName = this.refs.queryName.getDOMNode().value;
    let secretToken = UserStore.getUserDetails().secretToken;
    let query = codeMirror.getValue();

    // set limit if mode is in-memory
    if (!Config.isPersistent) query = setLimit(query);

    AdhocQueryActions.runQuery(secretToken, query, queryName);

    // show user the query was posted successfully and empty the queryName
    this.setState({ clientMessage: clientMessages.runQuery });
    this.refs.queryName.getDOMNode().value = '';
  }

  updateQuery (params) {
    let query = this._getSavedQueryDetails(params);
    if (!query) return;

    var options = {
      parameters: query.parameters,
      description: query.description,
      name: query.name
    };

    AdhocQueryActions
      .updateSavedQuery(query.secretToken, query.user, query.query,
        options, this.state.savedQueryId);

    this.setState({
      clientMessage: clientMessages.updateQuery,
      runImmediately: params && params.runImmediately
    });
  }

  saveQuery (params) {
    let query = this._getSavedQueryDetails(params);
    if (!query) return;

    var options = {
      parameters: query.parameters,
      description: query.description,
      name: query.name
    };

    AdhocQueryActions
      .saveQuery(query.secretToken, query.user, query.query, options);

    this.setState({
      clientMessage: clientMessages.saveQuery,
      runImmediately: params && params.runImmediately
    });
  }

  // internal which is called during save saved query & edit saved query
  _getSavedQueryDetails (params) {
    let queryName = this.refs.queryName.getDOMNode().value;
    if (!queryName) {
      this.setState({clientMessage: clientMessages.noName});
      return;
    }

    let secretToken = UserStore.getUserDetails().secretToken;
    let user = UserStore.getUserDetails().email;
    let query = codeMirror.getValue();

    return {
      secretToken: secretToken,
      user: user,
      query: query,
      parameters: params && params.parameters,
      description: params && params.description,
      name: queryName
    };
  }

  _onChange (hash) { // can be error/success OR it can be saved query params
    if (hash && hash.type) {
      this.setState({serverMessage: hash, clientMessage: null});

      if (hash.type === 'Error') return;
    } else {
      this.setState({serverMessage: null});
    }

    // renders the detail result component if server
    // replied with a query handle.
    // this should ideally happen only when the 'Run Query' button is
    // clicked, and its action updates the store with query-handle.
    let handle = AdhocQueryStore.getQueryHandle();
    if (handle) {
      this.setState({ clientMessage: null });

      var { router } = this.context;
      router.transitionTo('result', {handle: handle});
    }
  }

  _onChangeCubeStore () {
    // cubes
    let cubes = CubeStore.getCubes(); // hashmap
    Object.keys(cubes).forEach((cubeName) => {
      let cube = cubes[cubeName];
      codeMirrorHints[cubeName] = [];

      if (cube.measures && cube.measures.length) {
        cube.measures.forEach((measure) => {
          codeMirrorHints[cubeName].push(measure.name);
        });
      }
      if (cube.dimensions && cube.dimensions.length) {
        cube.dimensions.forEach((dimension) => {
          codeMirrorHints[cubeName].push(dimension.name);
        });
      }
    });

    codeMirror.options.hintOptions = { tables: codeMirrorHints };
  }

  _onChangeTableStore () {
    //  tables
    let databases = DatabaseStore.getDatabases() || [];
    let tables = databases.map(db => {
      if (TableStore.getTables(db)) {
        return {
          database: db,
          tables: TableStore.getTables(db)
        };
      }
    }).filter(item => { return !!item; }); // filtering undefined items

    tables.forEach(tableObject => {
      Object.keys(tableObject.tables).forEach(tableName => {
        let table = tableObject.tables[tableName];
        let qualifiedName = tableObject.database + '.' + tableName;
        codeMirrorHints[qualifiedName] = [];
        codeMirrorHints[tableName] = [];

        if (table.columns && table.columns.length) {
          table.columns.forEach((col) => {
            codeMirrorHints[qualifiedName].push(col.name);
            codeMirrorHints[tableName].push(col.name);
            codeMirrorHints[col.name] = [];
          });
        }
      });
    });

    codeMirror.options.hintOptions = { tables: codeMirrorHints };
  }

  _onChangeSavedQueryStore (hash) {
    if (!hash) return;

    var newState = _.assign({}, this.state);

    switch (hash.type) {
      case 'failure':
        newState.clientMessage = null;
        newState.serverMessage = hash.message;
        break;

      case 'success':
        // trigger to fetch the edited from server again
        let token = UserStore.getUserDetails().secretToken;
        if (hash.id) AdhocQueryActions.getSavedQueryById(token, hash.id);
        // means the query was saved successfully.

        // run immediately?
        if (newState.runImmediately && hash.id) {
          this.runSavedQuery(hash.id);
          newState.runImmediately = false;
        }

        // empty the state, clean the slate
        setCode('');
        this.refs.queryName.getDOMNode().value = '';
        newState = getEmptyState();
        newState.serverMessage = hash.message;
        break;

      case 'params':
        newState.params = hash.params.map(param => {
          return {
            name: param.name,
            dataType: param.dataType || 'STRING',
            collectionType: param.collectionType || 'SINGLE',
            defaultValue: param.defaultValue || null,
            displayName: param.displayName || param.name
          };
        });
        break;
    }

    this.setState(newState);
  }

  runSavedQuery (id) {
    let secretToken = UserStore.getUserDetails().secretToken;
    let parameters = this.state.params.map(param => {
      let object = {};
      object[param.name] = param.defaultValue;
      return object;
    });
    AdhocQueryActions.runSavedQuery(secretToken, id, parameters);
  }

  toggle () {
    this.setState({isCollapsed: !this.state.isCollapsed});
  }

  closeParamBox () {
    this.cancel();
  }

  saveParams (params) {
    !this.state.isModeEdit ? this.saveQuery(params) : this.updateQuery(params);
  }

  cancel () {
    setCode('');
    this.refs.queryName.getDOMNode().value = '';
    this.setState(getEmptyState());
  }
}

QueryBox.contextTypes = {
  router: React.PropTypes.func
};

export default QueryBox;
