/*
 * View representing a single row in the HistoryTableView
 */
var HistoryRowView = function(query) {
	var id = "history-row-view-" + HistoryRowView.instanceNo++;
	var model = query;

	/*
	 * Returns the query associated with this row
	 */
	this.getModel = function() {
		return model;
	};

	/*
	 * Generates the view to be rendered
	 */
	this.getView = function() {
		var historyRow = $("<tr>", {
			id: id
		});

		var submissionTime = $("<td>", {
			text: moment(model.getSubmissionTime()).fromNow(),
			title: moment(model.getSubmissionTime()).format('MMMM Do YYYY, h:mm:ss a')
		}).attr("data-sort-value", model.getSubmissionTime());

		var userQuery = $("<td>", {
			text: model.getUserQuery()
		});

		var status = $("<td>").append(
			$("<span>", {
				class: "label label-" + getStatusClass(),
				text: model.getQueryStatus().toLowerCase()
			}).attr("data-sort-value", model.getQueryStatus().toLowerCase())
		);

		var actions = $("<td>");
		var editButton = $("<button>", {
				class: "btn btn-info",
				title: "Edit/Re-run Query"
			})
			.prepend($("<span>", {
				class: "glyphicon glyphicon-pencil"
			}))
			.click(editFunction);
		if (model.getQueryStatus() === "SUCCESSFUL") {
			var resultButton = $("<button>", {
					class: "btn btn-success",
					title: "View Results"
				})
				.prepend($("<span>", {
					class: "glyphicon glyphicon-eye-open"
				}))
				.click(showResultFunction);

			actions.append(editButton);
			actions.append(resultButton);
		} else if (model.getQueryStatus() !== "FAILED" && model.getQueryStatus() !== "CANCELLED") {
			var cancelButton = $("<button>", {
					class: "btn btn-danger",
					title: "Cancel Query"
				})
				.prepend($("<span>", {
					class: "glyphicon glyphicon-trash"
				}))
				.click(function() {
					model.cancelQuery(null);
				});
			actions.append(cancelButton);
		} else {
			actions.append(editButton);
		}

		return historyRow
			.append(submissionTime)
			.append(userQuery)
			.append(status)
			.append(actions);
	};

	/*
	 * Call this method once the view has been rendered. Converts the query display into a
	 * read-only CodeMirror instance
	 */
	this.attachedToView = function() {
		var el = $("#" + id + " td:nth-child(2)");
		el.empty();
		console.log(el.get(0));
		var codeMirror = CodeMirror(el.get(0), {
			mode: "text/x-sql",
			lineWrapping: true,
			readOnly: true,
			value: model.getUserQuery()
		});
	};

	var getStatusClass = function() {
		if (model.getQueryStatus() === "SUCCESSFUL")
			return "success";

		if (model.getQueryStatus() === "CANCELLED")
			return "warning";

		if (model.getQueryStatus() === "FAILED")
			return "danger";

		return "primary";
	};

	var editFunction = function() {
		window.location.hash = "#query";
		window.loadPage();
		window.codeMirror.getDoc().setValue(model.getUserQuery());
	};

	var showResultFunction = function() {
		window.location.hash = "#query";
		window.loadPage();
		window.codeMirror.getDoc().setValue(model.getUserQuery());
		window.showQueryResults(model);
	}
};
HistoryRowView.instanceNo = 0;