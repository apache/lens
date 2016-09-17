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
  let currentDatabase = payload.database;
  cubes[currentDatabase] = cubes[currentDatabase] || {};
  payload.database && payload.cubes && payload.cubes.stringList &&
    payload.cubes.stringList.elements &&
    payload.cubes.stringList.elements.forEach(cube => {
      if (!cubes[currentDatabase][cube]) {
        cubes[currentDatabase][cube] = { name: cube, isLoaded: false };
      }
    });
}

function receiveCubeDetails (payload) {
  cubes[payload.database] = cubes[payload.database] || {};
  let cubeDetails = payload.cubeDetails && payload.cubeDetails.x_cube;
  let dimensions = null;
  let measures = null;
  if (cubeDetails.type == 'x_base_cube') {
    dimensions = cubeDetails.dim_attributes &&
      cubeDetails.dim_attributes.dim_attribute;
    measures = cubeDetails.measures &&
      cubeDetails.measures.measure;
  } else if (cubeDetails.type == 'x_derived_cube') {
    dimensions = cubeDetails.dim_attr_names && cubeDetails.dim_attr_names.attr_name;
    measures = cubeDetails.measure_names && cubeDetails.measure_names.measure_name;
  }
  cubes[payload.database][cubeDetails.name] = cubes[payload.database][cubeDetails.name] || { name: cubeDetails.name, isLoaded: false };
  cubes[payload.database][cubeDetails.name].measures = measures;
  cubes[payload.database][cubeDetails.name].dimensions = dimensions;
  if (cubeDetails.type == 'x_base_cube') {
    cubes[payload.database][cubeDetails.name].join_chains = cubeDetails.join_chains.join_chain;
    cubes[payload.database][cubeDetails.name].expressions = cubeDetails.expressions.expression;
    let join_chains_by_name = {};
    cubes[payload.database][cubeDetails.name].join_chains.map((join_chain)=>{
      join_chains_by_name[join_chain.name] = join_chain
    });
    cubes[payload.database][cubeDetails.name].join_chains_by_name = join_chains_by_name;
  }
  cubes[payload.database][cubeDetails.name].isLoaded = true;
}

let CHANGE_EVENT = 'change';
var cubes = {};
let CubeStore = assign({}, EventEmitter.prototype, {
  getCubes (currentDatabase) {
    return cubes[currentDatabase];
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
