/*
 * View to display a Cube or Dimension
 */
var MetaView = function(meta) {
	var model = meta;

	/*
	 * Generates the view to be rendered
	 */
	this.getView = function() {
		return $("<li>", {
			class: "list-group-item list-group-item-" + getClass(),
			text: model.getName() + ((model.getType() !== "cube" && model.getType() !== "dimtable") ? " (" + model.getType() + ")" : ""),
			type: model.getType(),
		}).prepend(
			$("<span>", {
				class: ((model.getType() === "cube" || model.getType() === "dimtable") ? "glyphicon glyphicon-chevron-right" : "")
			})
		).data("disp-value", model.getName());
	}

	var getClass = function() {
		if (model.getType() === "cube") {
			return "info";
		} else if (model.getType() === "dimtable") {
			return "success";
		}
		return "default";
	}
}