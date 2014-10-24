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
 * View used for displaying query history
 */
var HistoryTableView = function() {
	var id = "history-table-view-" + HistoryTableView.instanceNo++;
	var historyRowViews = [];

	/*
	 * Adds a row to the query history table
	 */
	this.addRow = function(historyRowView) {
		for (var i = 0; i < historyRowViews.length; i++) {
			if (historyRowViews[i].getModel().getHandle() === historyRowView.getModel().getHandle())
				return;
		}

		//Add only after data has been loaded
		historyRowView.getModel().setOnUpdatedListener(function() {
			historyRowViews.push(historyRowView);
			$("#" + id + " tbody").append(historyRowView.getView());
			historyRowView.attachedToView();
			$("#" + id + " th:nth-child(1)").data("sort-dir", "asc").trigger("click"); //Sort
		});
	};

	/*
	 * Generates the view to be rendered
	 */
	this.getView = function() {
		var table = $("<table>", {
			id: id,
			class: "table"
		});

		var tableHead = $("<thead>");
		tableHead.append(
			$("<tr>").append(
				$("<th>", {
					text: "Time",
					class: "col-md-1"
				})
				.attr("data-sort", "int")
				.attr("data-sort-default", "desc")
				.append(
					$("<span>", {
						class: "glyphicon glyphicon-sort"
					})
				)
			).append(
				$("<th>", {
					text: "Query",
					class: "col-md-8"
				})
			).append(
				$("<th>", {
					text: "Status"
				}).attr("data-sort", "string")
				.append(
					$("<span>", {
						class: "glyphicon glyphicon-sort"
					})
				)
			).append(
				$("<th>", {
					text: "Actions"
				})
			)
		);

		var tableBody = $("<tbody>");

		return table.append(tableHead).append(tableBody);
	};
};
HistoryTableView.instanceNo = 0;
