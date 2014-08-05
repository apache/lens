var Row = function() {
	var columns = [];

	this.addColumn = function(column) {
		columns.push(column);
	}

	this.getColumns = function() {
		return columns;
	}

};