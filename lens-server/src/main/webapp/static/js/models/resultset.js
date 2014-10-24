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
 * Represents the result set of a query.
 */
var ResultSet = function(queryHandle) {
	var totalRows = 0;
	var startIndex = 1;
	var fetchSize = 10;

	/*
	 * Fetch n number of results starting from start index
	 */
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
				if(totalRows === 1 && data["values"]["values"][1]["value"] === "PersistentResultSet") {
					callback(null);
					return;
				}
				console.log("Received response. Total rows: " + totalRows);
				var rows = [];
				for (var i = 0; i < data["values"]["values"].length; i++) {
					var row = new Row;
					var dataColumns = data["values"]["values"][i]["values"]["values"];
					for (var j = 0; j < dataColumns.length; j++) {
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

	/*
	 * Check if there are more rows available and then return the next set of rows
	 */
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
