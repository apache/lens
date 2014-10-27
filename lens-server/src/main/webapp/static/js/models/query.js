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
 * Represents a query submitted to the server. Stores and fetches all the
 * information about a query
 */
var Query = function(handle) {
	var queryHandle = handle;
	var queryURL = util.QUERY_URL + "/" + queryHandle;
	var queryStatus = "";
	var userQuery = "";
	var submissionTime = "";
	var statusMessage = "";
	var progress = 0;
	var resultSetAvailable = false;
	var priority = "";
	var onCompletedListener = null;
	var onUpdatedListener = null;
	var resultSet = null;
	var errorMessage = null;

	this.getQueryStatus = function() {
		return queryStatus;
	};

	this.getStatusMessage = function() {
		return statusMessage;
	}

	this.isQuerySubmitted = function() {
		return queryHandle !== null;
	};

	this.isResultSetAvailable = function() {
		return resultSetAvailable;
	}

	this.getProgress = function() {
		return progress;
	}

	this.getResultSet = function() {
		if (!resultSet)
			resultSet = new ResultSet(queryHandle);
		return resultSet;
	};

	this.getUserQuery = function() {
		return userQuery;
	}

	this.getSubmissionTime = function() {
		return submissionTime;
	}

	this.getDownloadURL = function() {
		return queryURL + "/httpresultset?sessionid=" + session.getSessionHandle()["publicId"];
	}

	/*
	 * Sets the onCompletedListener. onCompletedListener is called when the query has finished
	 * executing.
	 */
	this.setOnCompletedListener = function(listener) {
		onCompletedListener = listener;
	}

	/*
	 * Sets the onUpdatedListener. onUpdatedListener is called whenever any values of the query
	 * are updated
	 */
	this.setOnUpdatedListener = function(listener) {
		onUpdatedListener = listener;
	}

	/*
	 * Checks whether the query has finished executing. A query is considered to be completed
	 * if it has reached either SUCCESSFULL, FAILED or CANCELLED state
	 */
	this.isCompleted = function() {
		return queryStatus === "SUCCESSFUL" || queryStatus === "FAILED" || queryStatus === "CANCELLED";
	}

	this.getHandle = function() {
		return queryHandle;
	}

	this.getErrorMessage = function() {
		return errorMessage;
	}

	/*
	 * Cancels the query
	 */
	this.cancelQuery = function(callback) {
		$.ajax({
			url: queryURL,
			type: 'DELETE',
			data: {
				publicId: session.getSessionHandle()["publicId"]
			},
			dataType: 'json',
			success: function(data) {
				if (util.isFunction(callback))
					callback(data);
			}
		})
	}

	/*
	 * Fethces query information from the server and stores it
	 */
	var update = function(callback) {
		$.ajax({
			url: queryURL,
			type: 'GET',
			data: {
				publicId: session.getSessionHandle()["publicId"]
			},
			dataType: 'json',
			success: function(data) {
				if (data.hasOwnProperty("queryHandle") && data["queryHandle"].hasOwnProperty("handleId") && data["queryHandle"]["handleId"] === queryHandle) {
					queryStatus = data["status"]["status"];
					userQuery = data["userQuery"];
					submissionTime = data["submissionTime"];
					statusMessage = data["status"]["statusMessage"];
					progress = data["status"]["progress"];
					resultSetAvailable = data["status"]["isResultSetAvailable"];
					priority = data["priority"];
					errorMessage = data["status"]["errorMessage"];
				} else
					console.log("Error updating query data: " + data);
				if (util.isFunction(callback))
					callback();
			},
			error: function(jqXHR, textStatus, errorThrown) {
				console.log("Error querying query data: " + textStatus);
				if (util.isFunction(callback))
					callback();
			}
		});
	}

	//Fetch information about the query every 3 seconds
	var interval = window.setInterval(function() {
		if (queryStatus !== "SUCCESSFUL" && queryStatus !== "FAILED" && queryStatus !== "CANCELLED") {
			update(function() {
				if (util.isFunction(onUpdatedListener))
					onUpdatedListener();
			});
		} else {
			clearInterval(interval);
			if (util.isFunction(onCompletedListener))
				onCompletedListener();
		}
	}, 3000);

};
