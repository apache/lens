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
