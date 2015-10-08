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
import UserStore from '../stores/UserStore';
import LoginActions from '../actions/LoginActions';

import '../styles/css/login.css';

var error = false;

class Login extends React.Component {
  constructor (props) {
    super(props);
    this.handleSubmit = this.handleSubmit.bind(this);
    this._onChange = this._onChange.bind(this);
    this.state = {
      error: UserStore.isUserLoggedIn()
    };
  }

  componentDidMount () {
    UserStore.addChangeListener(this._onChange);
  }

  componentWillUnmount () {
    UserStore.removeChangeListener(this._onChange);
  }

  handleSubmit (event) {
    event.preventDefault();
    var email = this.refs.email.getDOMNode().value;
    var pass = this.refs.pass.getDOMNode().value;

    LoginActions.authenticate(email, pass);
  }

  render () {
    return (
      <section className='row' style={{margin: 'auto'}}>
        <form className='form-signin' onSubmit={this.handleSubmit}>
          <h2 className='form-signin-heading'>Sign in</h2>
          <label htmlFor='inputEmail' className='sr-only'>Email address</label>
          <input ref='email' id='inputEmail' className='form-control'
            placeholder='Email address' required autoFocus/>
          <label htmlFor='inputPassword' className='sr-only'>Password</label>
          <input ref='pass' type='password' id='inputPassword'
            className='form-control' placeholder='Password' required/>
          <button className='btn btn-primary btn-block'
            type='submit'>Sign in</button>
          {this.state.error && (
            <div className='alert-danger text-center'
              style={{marginTop: '5px', padding: '0px 3px'}}>
              <h5>Sorry, authentication failed.</h5>
              <small>{this.state.errorMessage}</small>
            </div>
          )}
        </form>
      </section>
    );
  }

  _onChange (errorHash) {
    if (errorHash) {
      error = true;

      // on error return immediately.
      // need not go to router for a transition
      return this.setState({
        errorMessage: errorHash.responseCode + ': ' +
          errorHash.responseMessage,
        error: true
      });
    }

    // user is authenticated here
    var { router } = this.context;
    var nextPath = router.getCurrentQuery().nextPath;

    if (nextPath) {
      router.replaceWith(nextPath);
    } else {
      router.replaceWith('/about');
    }
  }

}

Login.contextTypes = {
  router: React.PropTypes.func
};

export default Login;
