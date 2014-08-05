var Util = function() {

	this.isFunction = function(functionToCheck) {
		var getType = {};
		return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
	};

	this.SESSION_URL =  "http://localhost:19999/session/";
	this.QUERY_URL = "http://localhost:19999/queryuiapi/queries";
	this.META_URL = "http://localhost:19999/metastoreapi/tables";

	this.createMultipart = function(data) {
		var multiData = new FormData();
		$.each(data, function(key, value) {
			multiData.append(key, value);
		});

		return multiData;
	}
};