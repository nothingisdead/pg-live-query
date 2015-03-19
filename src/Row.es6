var _ = require('lodash')

var allRows = {};

function Row(index, data) {
	this.hash  = data._hash
	this.index = index

	if(!allRows[this.hash]) {
		allRows[this.hash] = data
	}
}

Row.prototype.get = function() {
	var data = _.clone(allRows[this.hash])

	data._index = this.index

	return data
}

module.exports = Row;
