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

import AppDispatcher from '../dispatcher/AppDispatcher';
import AppConstants from '../constants/AppConstants';
import assign from 'object-assign';
import { EventEmitter } from 'events';

var CHANGE_EVENT = 'change';
var userDetails = {
  isUserLoggedIn: false,
  email: '',
  secretToken: ''
};

// keeping these methods out of the UserStore class as
// components shouldn't lay their hands on them ;)
function authenticateUser (details) {
  userDetails = {
    isUserLoggedIn: true,
    email: details.email,
    // creating the session string which is passed with every request
    secretToken: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <lensSessionHandle>
        <publicId>${details.secretToken.lensSessionHandle.publicId}</publicId>
        <secretId>${details.secretToken.lensSessionHandle.secretId}</secretId>
      </lensSessionHandle>`
  };

  // store the details in localStorage if available
  if (window.localStorage) {
    let adhocCred = assign({}, userDetails, { timestamp: Date.now() });
    window.localStorage.setItem('adhocCred', JSON.stringify(adhocCred));
  }
}

function unauthenticateUser (details) {
  // details contains error code and message
  // which are not stored but passsed along
  // during emitChange()
  userDetails = {
    isUserLoggedIn: false,
    email: '',
    secretToken: ''
  };

  // remove from localStorage as well
  if (window.localStorage) localStorage.setItem('adhocCred', '');
}

// exposing only necessary methods for the components.
var UserStore = assign({}, EventEmitter.prototype, {
  isUserLoggedIn () {
    if (userDetails && userDetails.isUserLoggedIn) {
      return userDetails.isUserLoggedIn;
    } else if (window.localStorage && localStorage.getItem('adhocCred')) {
      // check in localstorage
      let credentials = JSON.parse(localStorage.getItem('adhocCred'));

      // check if it's valid or not
      if (Date.now() - credentials.timestamp > 1800000) return false;

      delete credentials.timestamp;
      userDetails = assign({}, credentials);

      return userDetails.isUserLoggedIn;
    }

    return false;
  },

  getUserDetails () {
    return userDetails;
  },

  logout () {
    unauthenticateUser();
    this.emitChange();
  },

  emitChange (errorHash) {
    this.emit(CHANGE_EVENT, errorHash);
  },

  addChangeListener (callback) {
    this.on(CHANGE_EVENT, callback);
  },

  removeChangeListener (callback) {
    this.removeListener(CHANGE_EVENT, callback);
  }
});

// registering callbacks with the dispatcher. So verbose?? I know right!
AppDispatcher.register((action) => {
  switch (action.actionType) {
    case AppConstants.AUTHENTICATION_SUCCESS:
      authenticateUser(action.payload);
      UserStore.emitChange();
      break;

    case AppConstants.AUTHENTICATION_FAILED:
      unauthenticateUser(action.payload);

      // action.payload => { responseCode, responseMessage }
      UserStore.emitChange(action.payload);
      break;
  }
});

export default UserStore;
