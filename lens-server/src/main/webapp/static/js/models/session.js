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
/*
 * Represents a user session.
 */
var Session = function() {
    var sessionHandle = null;
    var userName = "";

    //Check if cookie exists
    var docCookies = util.getCookies();
    if (docCookies.hasOwnProperty("publicId") && docCookies.hasOwnProperty("secretId") && docCookies.hasOwnProperty("userName")) {
        sessionHandle = {
            publicId: docCookies["publicId"],
            secretId: docCookies["secretId"]
        }
        userName = docCookies["userName"];
    }

    /*
     * Checks if the user has logged in
     */
    this.isLoggedIn = function() {
        return sessionHandle !== null && sessionHandle.hasOwnProperty("publicId") && sessionHandle.hasOwnProperty("secretId");
    };

    /*
     * Authenticates the user and generates a session handle
     */
    this.logIn = function(username, password, callback) {
        userName = username;
        $.ajax({
            url: util.SESSION_URL,
            type: 'POST',
            dataType: 'json',
            contentType: false,
            processData: false,
            cache: false,
            data: util.createMultipart({
                username: username,
                password: password
            }),
            success: function(data) {
                console.log(data);
                if (data.hasOwnProperty("publicId") && data.hasOwnProperty("secretId")) {
                    sessionHandle = data;
                    docCookies["publicId"] = sessionHandle["publicId"];
                    docCookies["secretId"] = sessionHandle["secretId"];
                    docCookies["userName"] = userName;
                    document.cookie="publicId=" + docCookies["publicId"];
                    document.cookie="secretId=" + docCookies["secretId"];
                    document.cookie="userName=" + docCookies["userName"];
                } else {
                    alert("The email or password you entered is incorrect.");
                    console.log("Error authenticating user: " + data);
                }
                if (util.isFunction(callback))
                    callback();
            },
            error: function(jqXHR, textStatus, errorThrown) {
                alert("The email or password you entered is incorrect.");
                console.log("Error authenticating user: " + textStatus);
                if (util.isFunction(callback))
                    callback();
            }
        });
    };

    /*
     * Fetches all the queries submitted by the user
     */
    this.getAllQueries = function(callback) {
        $.ajax({
            url: util.QUERY_URL,
            type: 'GET',
            dataType: 'json',
            data: {
                publicId: session.getSessionHandle()["publicId"],
                user: userName
            },
            success: function(data) {
                if (data !== null && data.length > 0) {
                    if (util.isFunction(callback))
                        callback(data);
                }
            }
        });
    };

    /*
     * Gets the metastore information from the server. Fetches different cubes and dimensions.
     */
    this.getAvailableMeta = function(callback) {
        $.ajax({
            url: util.META_URL,
            type: 'GET',
            dataType: 'json',
            data: {
                publicId: session.getSessionHandle()["publicId"]
            },
            success: function(tableList) {
                var metaArr = [];
                for (var item in tableList) {
                    var name = tableList[item].name;
                    var type = tableList[item].type;
                    var metaObj = new Meta(name, type);
                    metaArr.push(metaObj);
                }
                if (util.isFunction(callback))
                    callback(metaArr);
            }
        });
    };

    /*
     * Gets metastore information about the cube 'cubeName'
     */
    this.getCubeMeta = function(cubeName, callback) {
        $.ajax({
            url: util.META_URL + "/" + cubeName,
            type: 'GET',
            dataType: 'json',
            data: {
                publicId: session.getSessionHandle()["publicId"],
                type: "cube"
            },
            success: function(tableList) {
                var cubeArr = [];
                for (var item in tableList) {
                    var name = tableList[item].name;
                    var type = tableList[item].type;
                    var metaObj = new Meta(name, type);
                    cubeArr.push(metaObj);
                }
                if (util.isFunction(callback))
                    callback(cubeArr);
            }
        });
    };

    /*
     * Gets the metastore information about the dimension 'dimtableName'
     */
    this.getDimtableMeta = function(dimtableName, callback) {
        $.ajax({
            url: util.META_URL + "/" + dimtableName,
            type: 'GET',
            dataType: 'json',
            data: {
                publicId: session.getSessionHandle()["publicId"],
                type: "dimtable"
            },
            success: function(tableList) {
                var dimArr = [];
                for (var item in tableList) {
                    var name = tableList[item].name;
                    var type = tableList[item].type;
                    var metaObj = new Meta(name, type);
                    dimArr.push(metaObj);
                }
                if (util.isFunction(callback))
                    callback(dimArr);
            }
        });
    };

    /*
     * Searches the metastore for the provided keyword
     */
    this.searchMeta = function(keyword, callback) {
        $.ajax({
            url: util.SEARCH_URL,
            type: 'GET',
            dataType: 'json',
            data: {
                publicId: session.getSessionHandle()["publicId"],
                keyword: keyword
            },
            success: function(tableList) {
                var metaArr = [];
                for (var item in tableList) {
                    var name = tableList[item].name;
                    var type = tableList[item].type;
                    var childArr = [];
                    childArr = tableList[item].columns;
                    var metaObj = new Meta(name, type);
                    for (var col in childArr) {
                        var childName = childArr[col].name;
                        var childType = childArr[col].type;
                        var metaChildObj = new Meta(childName, childType);
                        metaObj.addChild(metaChildObj);
                    }
                    metaArr.push(metaObj);
                }
                if (util.isFunction(callback))
                    callback(metaArr);
            }
        });
    };

    this.getSessionHandle = function() {
        return sessionHandle;
    }

    this.logOut = function(callback) {
        var sessionVal = session.getSessionHandle()["publicId"];
        $.ajax({
                url: util.SESSION_URL + '/' + sessionVal,
                type: 'DELETE',
                contentType: "application/json; charset=utf-8",
                processData: false,
                success: function(data) {
                    console.log("successfully logged out");
                    callback();
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    console.log("Error in logout : " + textStatus);
                }
       });
    };

    /*
     * Submits the query to the server
     */
    this.submitQuery = function(query, callback) {
        if (this.isLoggedIn()) {
            //Submit query using ajax
            $.ajax({
                url: util.QUERY_URL,
                type: 'POST',
                dataType: 'json',
                contentType: false,
                processData: false,
                cache: false,
                data: util.createMultipart({
                    publicId: session.getSessionHandle()["publicId"],
                    query: query
                }),
                success: function(data) {
                    console.log("Request success");
                    var myQuery = null;

                    if (data.hasOwnProperty("type") && data.hasOwnProperty("handleId") && data["type"] === "queryHandle") {
                        console.log("Query submitted successfuly");
                        queryHandle = data["handleId"];
                        myQuery = new Query(queryHandle);
                    } else
                        console.log("Error sending query request: " + data);

                    if (util.isFunction(callback))
                        callback(myQuery);
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    console.log("Error sending query request: " + textStatus);
                    if (util.isFunction(callback))
                        callback(null);
                }
            });
        } else
            callback("User not logged in");
    };
};
