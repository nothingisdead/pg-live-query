const ROW_NUMBER = Symbol('ROW_NUMBER');
const Watcher    = require('./watcher');

class LiveQuery extends Watcher {
	// Adds a 'rows' event to the 'watch' handler
	query(sql) {
		const handler = this.watch(sql);
		const state   = {};

		handler.on('changes', (changes, cols) => {
			changes.forEach((change) => {
				if(change.data) {
					const row = {};

					// Set the row number symbol
					row[ROW_NUMBER] = change.rn;

					change.data.forEach((el, i) => {
						row[cols[i]] = el;
					});

					// Update the state
					state[change.id] = Object.freeze(row);
				}
				else {
					// Update the state
					delete state[change.id];
				}
			});

			const rows = [];

			for(const i in state) {
				rows.push(state[i]);
			}

			rows.sort((a, b) => {
				return a[ROW_NUMBER] - b[ROW_NUMBER];
			});

			handler.emit('rows', rows);
		});

		return handler;
	}
}

module.exports = LiveQuery;
