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
import AdhocQueryConstants from '../constants/AdhocQueryConstants';
import assign from 'object-assign';
import { EventEmitter } from 'events';

// private methods
function receiveCubes (payload) {
  payload.cubes.elements && payload.cubes.elements.forEach(cube => {
    if (!cubes[cube]) {
      cubes[cube] = { name: cube, isLoaded: false };
    }
  });
}

function receiveCubeDetails (payload) {
  let cubeDetails = payload.cubeDetails;

  let dimensions = cubeDetails.dim_attributes &&
    cubeDetails.dim_attributes.dim_attribute;
  let measures = cubeDetails.measures &&
    cubeDetails.measures.measure;

  cubes[cubeDetails.name].measures = measures;
  cubes[cubeDetails.name].dimensions = dimensions;
  cubes[cubeDetails.name].isLoaded = true;
}

let CHANGE_EVENT = 'change';
var cubes = {};

let CubeStore = assign({}, EventEmitter.prototype, {
  getCubes () {
    return cubes;
  },

  emitChange () {
    this.emit(CHANGE_EVENT);
  },

  addChangeListener (callback) {
    this.on(CHANGE_EVENT, callback);
  },

  removeChangeListener (callback) {
    this.removeListener(CHANGE_EVENT, callback);
  }
});

AppDispatcher.register((action) => {
  switch (action.actionType) {
    case AdhocQueryConstants.RECEIVE_CUBES:
      receiveCubes(action.payload);
      CubeStore.emitChange();
      break;

    case AdhocQueryConstants.RECEIVE_CUBE_DETAILS:
      receiveCubeDetails(action.payload);
      CubeStore.emitChange();
      break;

  }
});

export default CubeStore;
