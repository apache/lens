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
import { GridLoader } from 'halogen';

// TODO add warnings if size and margin props aren't sent.
class Loader extends React.Component {
  render () {
    return (
      <section style={{margin: '0 auto', maxWidth: '12%'}}>
        <GridLoader {...this.props} color='#337ab7'/>
      </section>
    );
  }
}

export default Loader;
