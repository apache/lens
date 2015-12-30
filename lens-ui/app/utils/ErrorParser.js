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

let ErrorParser = {
  getMessage (errorXML) {
    let errors = [];

    errors = Array.prototype.slice.call(errorXML.getElementsByTagName('error'))
      .map(error => {
        return {
          code: error.getElementsByTagName('code')[0].textContent,
          message: error.getElementsByTagName('message')[0].textContent
        };
      })
      .sort((a, b) => {
        return parseInt(a.code, 10) - parseInt(b.code, 10);
      })
      // removes duplicate error messages
      .filter((item, pos, array) => {
        return !pos || (item.code != (array[pos - 1] && array[pos - 1].code));
      })
      // removes not so helpful `Internal Server Error`
      .filter(error => {
        return error.code != 1001;
      });

    if (errors && errors.length == 0) {
      errors[0] = {};
      errors[0].code = 500;
      errors[0].message = 'Oh snap! Something went wrong. Please try again later.';
    }

    return errors;
  }
};

export default ErrorParser;
