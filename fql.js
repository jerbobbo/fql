var fs = require('fs');

// _readTable takes a string representing a table name
// and returns an array of objects, namely the rows.
// It does so by looking up actual files, reading them,
// and parsing them from JSON strings into JS objects.
function _readTable (tableName) {
	var folderName = __dirname + '/film-database/' + tableName;
	var fileNames = fs.readdirSync(folderName);
	var fileStrings = fileNames.map(function (fileName) {
		var filePath = folderName + '/' + fileName;
		return fs.readFileSync(filePath).toString();
	});
	var table = fileStrings.map(function (fileStr) {
		return JSON.parse(fileStr);
	});
	return table;
}

function merge (obj1, obj2) {

	var result = {};

	for(var key in obj1){
		result[key] = obj1[key];
	}
	for(key in obj2){
		result[key] = obj2[key];
	}

	return result;
}


function FQL (table) {
	this.table = table;
	this.indexTables = {};
}

FQL.prototype.exec = function() {
	return this.table;
};

FQL.prototype.count = function() {
	return this.table.length;
};

FQL.prototype.limit = function(size){
	return new FQL(this.table.slice(0, size));
};

FQL.prototype.where = function(object) {
	var returnArr = [];
	this.table.forEach(function(row) {

		var numTrue = 0;

		for (var key in object) {

			if ( typeof object[key] === 'function') {
				if (object[key](row[key]))
					numTrue++;
			}

			else if (row[key] === object[key])
				numTrue++;
		}
		if (numTrue === Object.keys(object).length)
			returnArr.push(row);

	});

	return new FQL(returnArr);
};

FQL.prototype.select = function (cols){
	var returnArr = [];

	this.table.forEach(function(row){
		var newObj = {};

		for(var i=0;i<cols.length;i++){
			newObj[cols[i]] = row[cols[i]];
		}
		returnArr.push(newObj);
	});

	return new FQL(returnArr);
};

FQL.prototype.order = function(param){

	function sort(a, b){
			if(a[param] > b[param])
				return 1;
			else if(a[param] < b[param])
				return -1;
			else
				return 0;
		}

	return new FQL(this.table.sort(sort));
};

FQL.prototype.left_join = function(table_left, testFunc) {
	var retArray = [];
	var that = this;
	table_left.table.forEach(function(row_left) {
		that.table.forEach(function(row_right) {
			if (testFunc(row_right, row_left))
				retArray.push(merge(row_left, row_right));

		});
	});
	return new FQL(retArray);
};

FQL.prototype.addIndex = function(column) {
	var newIndex = {};
	for (var i=0; i < this.table.length; i++){
		var row = this.table[i];

		if (!newIndex[row[column]]) {
			newIndex[row[column]] = [];
		}
		newIndex[row[column]].push(i);

	}

	this.indexTables[column] = newIndex;
};

FQL.prototype.getIndicesOf = function(column, searchStr) {
	if (!this.indexTables[column])
		return undefined;
	var index = this.indexTables[column];
	return index[searchStr];
};

module.exports = {
	FQL: FQL,
	merge: merge,
	_readTable: _readTable
};
