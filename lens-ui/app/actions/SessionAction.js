import AppDispatcher from "../dispatcher/AppDispatcher";
import SessionConstants from "../constants/SessionConstants";
import SessionAdapter from "../adapters/SessionAdapter";

let SessionActions = {
    getSessions() {
        SessionAdapter.getSessions()
            .then(function (response) {
                AppDispatcher.dispatch({
                    actionType: SessionConstants.RECEIVE_SESSIONS,
                    payload: { sessions : response }
                });
            }, function (error) {
                // propagating the error message
                AppDispatcher.dispatch({
                    actionType: SessionConstants.RECEIVE_SESSIONS_FAILED,
                    payload: {
                        responseCode: error.status,
                        responseMessage: error.statusText
                    }
                });
            });
    }
};

export default SessionActions;