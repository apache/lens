/*
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
var Util = function() {

	this.isFunction = function(functionToCheck) {
		var getType = {};
		return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
	};

	this.SESSION_URL = "/uisession/";
	this.QUERY_URL = "/queryuiapi/queries";
	this.META_URL = "/metastoreapi/tables";
	this.SEARCH_URL = "/metastoreapi/searchablefields";

	this.createMultipart = function(data) {
		var multiData = new FormData();
		$.each(data, function(key, value) {
			multiData.append(key, value);
		});

		return multiData;
	};

	this.getCookies = function() {
		var cookieObj = {};
		var cookies = document.cookie;
		cookieArr = cookies.split(';');
		if(cookieArr.length > 0) {
			for (var i = cookieArr.length - 1; i >= 0; i--) {
				var tempArr = cookieArr[i].trim().split('=');
				if(tempArr.length === 2 && tempArr[1] !== "")
					cookieObj[tempArr[0]] = tempArr[1];
			};
		};

		return cookieObj;
	};
};
