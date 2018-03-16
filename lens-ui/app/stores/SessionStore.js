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
import assign from "object-assign";
import SessionConstants from "../constants/SessionConstants";
import AppDispatcher from "../dispatcher/AppDispatcher";
import {EventEmitter} from "events";

let CHANGE_EVENT = 'change';
var sessionList = [];

let Sessions = assign({}, EventEmitter.prototype, {
    getSessions() {
        return sessionList;
    },

    emitChange (hash) {
        this.emit(CHANGE_EVENT, hash);
    },

    addChangeListener (callback) {
        this.on(CHANGE_EVENT, callback);
    },

    removeChangeListener (callback) {
        this.removeListener(CHANGE_EVENT, callback);
    }
});

function receiveSessions(payload) {
    var list = payload.sessions;
    sessionList = list.map(function(l) {
        return l['userSessionInfo'];
    })
}
AppDispatcher.register((action) => {
    switch (action.actionType) {
        case SessionConstants.RECEIVE_SESSIONS:
            receiveSessions(action.payload);
            break;
    }
    Sessions.emitChange();
});

export default Sessions;
