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
import { Link } from 'react-router';

import UserStore from '../stores/UserStore';

class Header extends React.Component {
  constructor () {
    super();
    this.state = {userName: null};
    this._onChange = this._onChange.bind(this);
  }

  componentDidMount () {
    UserStore.addChangeListener(this._onChange);

    // this component mounts later and CHANGE_EVENT has elapsed
    // calling _onChange manually once to refresh the value
    // FIXME is this wrong?
    this._onChange();
  }

  componentWillUnmount () {
    UserStore.removeChangeListener(this._onChange);
  }

  render () {
    return (
      <nav className='navbar navbar-inverse navbar-static-top'>
        <div className='container'>
          <div className='navbar-header'>
            <button type='button' className='navbar-toggle collapsed'
                data-toggle='collapse' data-target='#navbar'
                aria-expanded='false' aria-controls='navbar'>
              <span className='sr-only'>Toggle navigation</span>
              <span className='icon-bar'></span>
              <span className='icon-bar'></span>
              <span className='icon-bar'></span>
            </button>
            <Link className='navbar-brand' to='app'>LENS Query<sup>&beta;</sup></Link>
          </div>
          <div id='navbar' className='collapse navbar-collapse'>
            <ul className='nav navbar-nav'>
              <li><Link to='about'>About</Link></li>
            </ul>

            { this.state.userName &&
              (<ul className='nav navbar-nav navbar-right'>
                <li>
                  <Link to='logout' className='glyphicon glyphicon-log-out'
                    title='Logout'>
                    <span> {this.state.userName}</span>
                  </Link>
                </li>
              </ul>)
            }

          </div>
        </div>
      </nav>
    );
  }

  _onChange () {
    this.setState({userName: UserStore.getUserDetails().email});
  }
}

export default Header;
