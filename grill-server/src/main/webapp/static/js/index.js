var codeMirror = CodeMirror.fromTextArea(document.getElementById("query"), {
    mode: "text/x-sql",
    lineWrapping: true
 });

var dblclickFunction = function(e) {
    e.stopPropagation();
    console.log(e.target);
    if( e.target !== this)
        return;
    //$(this).children().removeAttr('onclick');
    $(this).data('double', 2);
    console.log($(this).get(0));
	var text = $(this).data("disp-value");
	// var old = codeMirror.getDoc().getValue();
	// codeMirror.getDoc().setValue(old + text);
	codeMirror.getDoc().replaceSelection(text);
};

var expandFunction = function(cubedata, oldElement) {
    var newElement = $("<ul>", {});
    for(var i = 0; i < cubedata.length; i++) {
        var metaView = new MetaView(cubedata[i]);
        newElement.append(metaView.getView().dblclick(dblclickFunction));
        //Array.prototype.slice.call($("#meta-views")[0].children).splice(insertIndex + i, 0, metaView.getView());
    }
    oldElement.append(newElement);
};

var util = new Util;
var session = new Session;
var historyTableView = new HistoryTableView;
$("#historyui div").append(historyTableView.getView());
$("#historyui div table").stupidtable().bind("aftertablesort", function(event, data) {
	var el = $("#historyui div table th:nth-child(" + (data.column + 1) + ") span");
	$("#historyui div table th span.glyphicon").attr("class", "glyphicon glyphicon-sort");

	if(data.direction === "asc")
		el.attr("class", "glyphicon glyphicon-sort-by-attributes");
	else if(data.direction === "desc")
		el.attr("class", "glyphicon glyphicon-sort-by-attributes-alt");
});

var setEnableForm = function(enable) {
	codeMirror.setOption("readOnly", !enable);
	codeMirror.setOption("nocursor", !enable);
	$("#query-form button").attr("disabled", !enable);
	if(enable)
		$("#query-form .loading").hide();
	else
		$("#query-form .loading").show();
}

var loadPage = function() {
	//Hidden by default
	$("#query-form .loading").hide();
	$("#queryui, #loginui, #historyui").hide();
	$("#navlinks .active").removeClass("active");

	window.setEnableForm(true);
	while($("#query-form").next().next().length > 0) //Remove results table and pagination
		$("#query-form").next().next().remove();

	var page = window.location.hash.substr(1);
	if(!session.isLoggedIn()) {
		//Show login UI
		$("#loginui").show();
	}
	else if(page === "history") {
		$("#queryui, #historyui").show();
		$("#query-ui-content").hide();
		$("#navlinks li").last().addClass("active");
		session.getAllQueries(function(data) {
			for(var i = 0; i < data.length; i++) {
				var query = new Query(data[i]["handleId"]);
				var historyRowView = new HistoryRowView(query);
				historyTableView.addRow(historyRowView);
			}
		});
	}
	else {
		$("#queryui").show();
		$("#query-ui-content").show();
		$("#navlinks li").first().addClass("active");
		$("#meta-views").empty();
		session.getAvailableMeta(function(data) {
			for(var i = 0; i < data.length; i++) {
				var metaView = new MetaView(data[i]);
				$("#meta-views").append(metaView.getView());
			}
			$("#meta-views li").click(function(e) {
			    //$(this).children().removeAttr('onclick');
			    if( e.target !== this && e.target !== $(this).get(0).firstChild)
                       return;
			    e.stopPropagation();
                var that = this;
                setTimeout(function() {
                    var dblclick = parseInt($(that).data('double'), 10);
                    if (dblclick > 0) {
                        $(that).data('double', dblclick-1);
                    } else {
                        var insertIndex = $(that).parent().children().index(that);
        			    console.log($(that)[0].lastChild instanceof Text);
                        if($(that)[0].lastChild instanceof Text)
                        {
                        console.log("First Click");

        			    var currentElement = $(that);

        			        //console.log(newElement);
        			    if($(that)[0].type === "cube") {
        			        $(that).get(0).firstChild.className = "glyphicon glyphicon-chevron-down";
        			        session.getCubeMeta($(that).text(), function(cubedata){
        			            window.expandFunction(cubedata, currentElement);
                            });
                        }
                        else if($(that)[0].type === "dimtable") {
                            $(that).get(0).firstChild.className = "glyphicon glyphicon-chevron-down";
                            session.getDimtableMeta($(that).text(), function(cubedata){
        			            window.expandFunction(cubedata, currentElement);
                   	        });
                        }

                        }
                        else
                        {
                            $(that).get(0).firstChild.className = "glyphicon glyphicon-chevron-right";
                            console.log("Second Click");
                            while(!($(that)[0].lastChild instanceof Text))
                            {
                                $(that)[0].removeChild($(that)[0].lastChild);
                            }
                        }
                    }
                }, 300);
            }).dblclick(dblclickFunction);
		});
	}
}
loadPage();

var QueryStatusView = function(query) {
	var id = "query-status-view-" + QueryStatusView.instanceNo++;
	var model = query;
	var text = model.getStatusMessage();

	this.updateView = function() {
		text = model.getStatusMessage() + ((model.getQueryStatus() === "FAILED")? ". Reason: " + model.getErrorMessage() : "");
		$("#" + id).text(text);
	}
	
	this.getView = function() {
		return $("<span>", {id: id, text: text});
	}
};
QueryStatusView.instanceNo = 0;

//for histogram
    function modalFunction(data, title) {
	var chartData = [];
	var chartTitle = ""+title;
	for (var i = 0; i < data.length; i++)
	{
    	    chartData[i] = data[i].slice();
	}
	this.getData = function(){
            return chartData;
    	};

    	this.setData = function(data){
    	    for (var i = 0; i < data.length; i++)
	    {
    	    	chartData[i] = data[i].slice();
	    }
    	};
	    this.getTitle = function(){
            return chartTitle;
    	};

    	this.setTitle = function(title){
    	    chartTitle = ""+title;
    	};
    }


    function show_tooltip(x, y, contents) {
    	$('<div id="bar_tooltip">' + contents + '</div>').css({
        	top: y - 120,
        	left: x - 350,
    	}).appendTo($("#myModalCanvas")).fadeIn();
    }

	var previous_point = null;

    function hoverOverGrid(event, pos, item) {
	if (item) {

	        if (previous_point != item.dataIndex) {
	            previous_point = item.dataIndex;

	            $("#bar_tooltip").remove();

	            var y = item.datapoint[1];

	            show_tooltip(item.pageX, item.pageY,
	                "<div style='text-align: center;'><b> Value: " + y + "</div>");
	        }
	    } else {
	        $("#bar_tooltip").remove();
	        previous_point = null;
	    }
    }

     function displayChart(modal) {
	$.plot($("#myModalCanvas"), [modal.getData()], {
		grid: {
		        hoverable: true
		},
        	series: {
            	   bars: { show: true }
		}
	});
	$('#myModal').modal('toggle');
    $('#myModal').modal('show');
    //$('#myModal').modal('hide');
	document.getElementById('myModalLabel').innerHTML = modal.getTitle();
	$("#myModalCanvas").bind("plothover", hoverOverGrid);
    }


var TableResultView = function() {
	var id = "table-result-view-" + TableResultView.instanceNo++;
	var rows = [];

	this.updateView = function(rows) {
		$("#" + id).empty();

		if(rows && rows.length <= 0)
			return;

		//Add header
		$("#" + id).append($("<thead>").append($("<tr>")));
		for(var i = 0; i < rows[0].getColumns().length; i++) {
			$("#" + id + " thead tr").append($("<th>", {text: rows[0].getColumns()[i]}));
		};

		//Add body
		$("#" + id).append($("<tbody>"));
		for(var i = 1; i < rows.length; i++) {
			var tRow = $("<tr>");
			var columns = rows[i].getColumns();
			for(var j = 0; j < columns.length; j++) {
				tRow.append($("<td>", {text: columns[j]}));
			};
			$("#" + id + " tbody").append(tRow);
		};
	}

	this.addClickFunction = function() {
    		console.log("Trying to access column: " + id);
    		var table = $("#" + id);
            $("#" + id + " thead tr th").click(function(){
                var index = $(this)[0].cellIndex;
                var data = [];
                $("#" + id + " tbody tr").each( function(){
                   //add item to array
                   data.push([$(this)[0].rowIndex, parseInt($(this)[0].cells[index].firstChild.data, 10)] );

                });
                displayChart(new modalFunction(data,  $(this)[0].textContent));
            });
    	}

    	this.getId = function() {
    	    return id;
    	}

	this.getView = function() {
		return $("<table>", {id: id, class: "table table-bordered paginated"});
	}
};
TableResultView.instanceNo = 0;

var showQueryResults = function(queryObj) {
	var resultView = new TableResultView;
	while($("#query-form").next().next().length > 0)
		$("#query-form").next().next().remove();

	var rs = queryObj.getResultSet();
	rs.getNextRows(function(rows) {
		console.log("Got next rows");
		if(rows === null) {
			//No results
			$("#query-form").next().after($("<p>", {text: "No results found"}));
			return;
		}
		$("#query-form").next().after(resultView.getView());
		resultView.updateView(rows);
        resultView.addClickFunction();//to add histogram to a column
		window.paginate();
	});
}

$("#query-form").submit(function(event) {
	event.preventDefault(); 

	//Disable UI components
	setEnableForm(false);

	var query = $("#query").val().trim();

	//Perform basic checks
	if(query) {
		session.submitQuery(query, function(queryObj) {
			if(queryObj) {
				var queryStatusView = new QueryStatusView(queryObj);
				if($("#query-form .loading").next().length > 0)
					$("#query-form .loading").next().remove();
				$("#query-form .loading").after(queryStatusView.getView());

				queryObj.setOnUpdatedListener(queryStatusView.updateView);
				queryObj.setOnCompletedListener(function() {
					setEnableForm(true);
					//Display results
					console.log("Completed");
					if(queryObj.getQueryStatus() === "SUCCESSFUL") {
						window.showQueryResults(queryObj);
					}
				});
			}
			else {
				//Problem submitting query. Reset UI and display message
				setEnableForm(true);
			}
		});
	}
	else {
		//No query. Reset UI
		setEnableForm(true);
	}
});

$("#login-form").submit(function(event){
	event.preventDefault();

	var email = $("#email").val();
	var password = $("#password").val();

	if(!email) {
		$("#email").addClass("error");
		return;
	}
	$("#email").removeClass("error");

	if(!password) {
		$("#password").addClass("error");
		return;
	}
	$("#password").removeClass("error");

	$("#email, #password, #login-btn").attr("disabled", true);

	session.logIn(email, password, function() {
		window.location.reload();
	});
});

$("#navlinks li a").click(function(event) {
	event.preventDefault();
	window.location.hash = this.hash;
	loadPage();
});

$("#meta-input").keyup(function() {
	var searchTerm = $(this).val();
	if(searchTerm === null || searchTerm === "") {
		$("#meta-views").empty();
		session.getAvailableMeta(function(data) {
        			for(var i = 0; i < data.length; i++) {
        				var metaView = new MetaView(data[i]);
        				$("#meta-views").append(metaView.getView());
        			}
        			$("#meta-views li").click(function(e) {
        			    //$(this).children().removeAttr('onclick');
        			    if( e.target !== this && e.target !== $(this).get(0).firstChild)
                               return;
        			    e.stopPropagation();
                        var that = this;
                        setTimeout(function() {
                            var dblclick = parseInt($(that).data('double'), 10);
                            if (dblclick > 0) {
                                $(that).data('double', dblclick-1);
                            } else {
                                var insertIndex = $(that).parent().children().index(that);
                			    console.log($(that)[0].lastChild instanceof Text);
                                if($(that)[0].lastChild instanceof Text)
                                {
                                console.log("First Click");

                			    var currentElement = $(that);

                			        //console.log(newElement);
                			    if($(that)[0].type === "cube") {
                			        $(that).get(0).firstChild.className = "glyphicon glyphicon-chevron-down";
                			        session.getCubeMeta($(that).text(), function(cubedata){
                			            window.expandFunction(cubedata, currentElement);
                                    });
                                }
                                else if($(that)[0].type === "dimtable") {
                                    $(that).get(0).firstChild.className = "glyphicon glyphicon-chevron-down";
                                    session.getDimtableMeta($(that).text(), function(cubedata){
                			            window.expandFunction(cubedata, currentElement);
                           	        });
                                }

                                }
                                else
                                {
                                    $(that).get(0).firstChild.className = "glyphicon glyphicon-chevron-right";
                                    console.log("Second Click");
                                    while(!($(that)[0].lastChild instanceof Text))
                                    {
                                        $(that)[0].removeChild($(that)[0].lastChild);
                                    }
                                }
                            }
                        }, 300);
                    }).dblclick(dblclickFunction);
        		});
	}
	else {
		session.searchMeta(searchTerm, function(data) {
			$("#meta-views").empty();
			for(var i = 0; i < data.length; i++) {
				var metaView = new MetaView(data[i]);
				$("#meta-views").append(metaView.getView());
				   var newElement = $("<ul>", {});
				   var subdata = data[i].getColumns();
				   for(var j=0; j< subdata.length; j++) {
				      var submetaView = new MetaView(subdata[j]);
				        newElement.append(submetaView.getView());
				}
				$("#meta-views").append(newElement);
			}

			$("#meta-views li").dblclick(function(event) {
				var text = $(this).data("disp-value");
				// var old = codeMirror.getDoc().getValue();
				// codeMirror.getDoc().setValue(old + text);
				codeMirror.getDoc().replaceSelection(text);
			});
		});
	}

});

var paginate = function() {
	$('table.paginated').each(function() {
	    var currentPage = 0;
	    var numPerPage = 10;
	    var $table = $(this);
	    $table.bind('repaginate', function() {
	        $table.find('tbody tr').hide().slice(currentPage * numPerPage, (currentPage + 1) * numPerPage).show();
	    });
	    $table.trigger('repaginate');
	    var numRows = $table.find('tbody tr').length;
	    var numPages = Math.ceil(numRows / numPerPage);
	    var $pager = $('<ul class="pagination"></ul>');
	    for (var page = 0; page < numPages; page++) {
	        $('<li class="page-number"></li>').append($("<a>", {text:page + 1})).bind('click', {
	            newPage: page
	        }, function(event) {
	            currentPage = event.data['newPage'];
	            $table.trigger('repaginate');
	            $(this).addClass('active').siblings().removeClass('active');
	        }).appendTo($pager).addClass('clickable');
	    }
	    $pager.insertAfter($table).find('li.page-number:first').addClass('active');
	});
}