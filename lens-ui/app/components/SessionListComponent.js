/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React from "react";
import Session from "../stores/SessionStore";
import SessionAction from "../actions/SessionAction";
import {BootstrapTable, TableHeaderColumn} from "react-bootstrap-table";

class SessionList extends React.Component {
    constructor(props) {
        super(props);
        this.state = {sessionList: [], sessionReceived: false};
        this._onChange = this._onChange.bind(this);
        SessionAction.getSessions();
    }

    componentDidMount() {
        Session.addChangeListener(this._onChange);
    }

    componentWillUnmount() {
        Session.removeChangeListener(this._onChange);
    }

    componentWillReceiveProps(props) {
    }

    _onChange() {
        var sessions = Session.getSessions();
        var mod = sessions.map(function(l){
            var acq = l['activeQueries'];
            if (acq == undefined){
                l['queries'] = 0
            } else{
                l['queries'] = acq.length;
            }
            l['creationTime'] = (new Date(l['creationTime'])).toString();
            l['lastAccessTime'] = (new Date(l['lastAccessTime'])).toString();
        });
        this.setState({sessionList: Session.getSessions(), sessionReceived: true});
    }

    renderShowsTotal(start, to, total) {
        return (
            <p style={ { color: 'blue' } }>
                From { start } to { to }, totals is { total }&nbsp;&nbsp;(its a customize text)
            </p>
        );
    }
    render() {
        var options = {
            page: 1,  // which page you want to show as default
            sizePerPageList: [50, 100, 150], // you can change the dropdown list for size per page
            sizePerPage: 50,  // which size per page you want to locate as default
            pageStartIndex: 0, // where to start counting the pages
            sortOrder : 'desc',
            sortName : null,
            prePage: 'Prev', // Previous page button text
            nextPage: 'Next', // Next page button text
            firstPage: 'First', // First page button text
            lastPage: 'Last', // Last page button text
            paginationShowsTotal: this.renderShowsTotal  // Accept bool or function
      //      hideSizePerPage: true //> You can hide the dropdown for sizePerPage
        };
        var selectRowProp = {
            mode: "checkbox",  //checkbox for multi select, radio for single select.
            clickToSelect: true,   //click row will trigger a selection on that row.
            bgColor: "rgb(238, 193, 213)"   //selected row background color
        };
        return (
            <section>
                <div className='container-fluid'>
                    <BootstrapTable data={this.state.sessionList} pagination={true} striped={true} search={true}
                                    columnFilter={true}
                                    hover={true}
                                    condensed={true} options={options}>
                        <TableHeaderColumn dataField="handle" isKey={true} dataAlign="center"
                                           dataSort={true}>Session Handle</TableHeaderColumn>
                        <TableHeaderColumn dataField="userName" dataSort={true}>User Name</TableHeaderColumn>
                        <TableHeaderColumn dataField="queries" dataSort={true}>Number of Queries</TableHeaderColumn>
                        <TableHeaderColumn dataField="creationTime"
                                           dataSort={true}>Creation Time</TableHeaderColumn>
                        <TableHeaderColumn dataField="lastAccessTime"
                                           dataSort={true}>Last Access Time</TableHeaderColumn>
                    </BootstrapTable>
                </div>
            </section>
        );
    }
}

export default SessionList;
