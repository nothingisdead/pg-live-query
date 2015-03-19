var _ = require('lodash')

var allRows = {};

function Row(index, data) {
	if(data instanceof Row) {
		this.hash = data.hash
	}
	else {
		this.hash  = data._hash
	}

	this.index = index

	if(!allRows[this.hash]) {
		allRows[this.hash] = _.clone(data)
	}
}

Row.prototype.get = function() {
	return _.extend(allRows[this.hash], {
		_index : this.index
	})
}

module.exports = Row;
