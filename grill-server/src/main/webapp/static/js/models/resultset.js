var ResultSet = function(queryHandle) {
	var totalRows = 0;
	var startIndex = 1;
	var fetchSize = 10;

	//Fetch n number of results starting from start index
	this.getRows = function(start, n, callback) {
		$.ajax({
			url: util.QUERY_URL + "/" + queryHandle + "/resultset",
			type: 'GET', 
			data: {
				publicId: session.getSessionHandle()["publicId"],
				queryHandle: queryHandle,
				pageNumber: 1,
				fetchSize: 10
			},
			dataType: 'json',
			success: function(data) {
				totalRows = data["values"]["values"].length - 1;
				console.log("Received response. Total rows: " + totalRows);
				var rows = [];
				for (var i = 0; i < data["values"]["values"].length; i++) {
					var row = new Row;
					var dataColumns = data["values"]["values"][i]["values"]["values"];
					for(var j = 0; j < dataColumns.length; j++) {
						console.log("i: " + j + ". dataColumns[i]: " + dataColumns[j]);
						row.addColumn(dataColumns[j]["value"]);
					}
					rows.push(row);
				};

				callback(rows);
			},
			error: function() {
				//TODO
				callback(null);
			}
		});
	};

	//Check if there are more rows available and then return the next rows
	this.getNextRows = function(callback) {
		this.getRows(startIndex, fetchSize, callback);
		startIndex += fetchSize;
	};

	this.setFetchSize = function(size) {
		fetchSize = size;
	};

	this.getFetchSize = function() {
		return fetchSize;
	};

	this.reset = function() {
		startIndex = 1;
	};

	this.setTotalRows = function(total) {
		totalRows = total;
	};

	this.getTotalRows = function() {
		return totalRows;
	};
};