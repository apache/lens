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
