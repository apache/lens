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
		if(!resultSet)
			resultSet = new ResultSet(queryHandle);
		return resultSet;
	};

	this.getUserQuery = function() {
		return userQuery;
	}

	this.getSubmissionTime = function() {
		return submissionTime;
	}

	this.setOnCompletedListener = function(listener) {
		onCompletedListener = listener;
	}

	this.setOnUpdatedListener = function(listener) {
		onUpdatedListener = listener;
	}

	this.isCompleted = function() {
		return queryStatus === "SUCCESSFUL" || queryStatus === "FAILED" || queryStatus === "CANCELLED";
	}

	this.getHandle = function() {
		return queryHandle;
	}

	this.getErrorMessage = function() {
		return errorMessage;
	}

	this.cancelQuery = function(callback) {
		$.ajax({
			url: queryURL,
			type: 'DELETE',
			data: {publicId: session.getSessionHandle()["publicId"]},
			dataType: 'json',
			success: function(data) {
				if(util.isFunction(callback))
					callback(data);
			}
		})
	}

	var update = function(callback) {
		$.ajax({
			url: queryURL,
			type: 'GET',
			data: {publicId: session.getSessionHandle()["publicId"]},
			dataType: 'json',
			success: function(data) {
				if(data.hasOwnProperty("queryHandle") && data["queryHandle"].hasOwnProperty("handleId") && data["queryHandle"]["handleId"] === queryHandle) {
					queryStatus = data["status"]["status"];
					userQuery = data["userQuery"];
					submissionTime = data["submissionTime"];
					statusMessage = data["status"]["statusMessage"];
					progress = data["status"]["progress"];
					resultSetAvailable = data["status"]["isResultSetAvailable"];
					priority = data["priority"];
					errorMessage = data["status"]["errorMessage"];
				}
				else
					console.log("Error updating query data: " + data);
				if(util.isFunction(callback))
					callback();
			},
			error: function(jqXHR, textStatus, errorThrown) {
				console.log("Error querying query data: " + textStatus);
				if(util.isFunction(callback))
					callback();
			}
		});
	}

	var interval = window.setInterval(function() {
		if(queryStatus !== "SUCCESSFUL" && queryStatus !== "FAILED" && queryStatus !== "CANCELLED") {
			update(function() {
				if(util.isFunction(onUpdatedListener))
					onUpdatedListener();
			});
		}
		else {
			clearInterval(interval);
			if(util.isFunction(onCompletedListener))
				onCompletedListener();
		}
	}, 3000);

};