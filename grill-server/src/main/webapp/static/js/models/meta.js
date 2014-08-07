/*
 * Represents the Hive metastore information. Used to store information cube or dimension tables
 */
var Meta = function(metaName, metaType) {
    var name = metaName;
    var type = metaType;
    var columns = [];

    this.getName = function() {
        return name;
    }

    this.getType = function() {
        return type;
    }

    this.getColumns = function() {
        return columns;
    }

    this.addChild = function(child) {
        columns.push(child);
    }
}