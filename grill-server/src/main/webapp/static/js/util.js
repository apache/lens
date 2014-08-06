var Util = function() {

	this.isFunction = function(functionToCheck) {
		var getType = {};
		return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
	};

	this.SESSION_URL = "http://localhost:19999/session/";
	this.QUERY_URL = "http://localhost:19999/queryuiapi/queries";
	this.META_URL = "http://localhost:19999/metastoreapi/tables";

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