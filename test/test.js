const fs        = require('fs');
const tap       = require('tap');
const Pool      = require('pg').Pool;
const LiveQuery = require('../index');
const helpers   = require('../helpers');

// Get the database connection settings
const test_file = `${__dirname}/test.json`;

const connection_message = [
	'To test this package, please define connection settings in',
	test_file
].join('\n');

let test;

try {
	test = JSON.parse(fs.readFileSync(test_file) + '');
}
catch(e) {
	console.warn(connection_message);
	process.exit();
}

const maybeRun = (queries, client) => {
	if(!queries || !queries.length) {
		return Promise.resolve([]);
	}

	return helpers.queries(client, queries);
};

const ev = [ 'insert', 'update', 'delete', 'changes', 'rows' ];

const run = (tests, client) => {
	const lq   = new LiveQuery(client);
	const test = tests.shift();

	return new Promise((resolve, reject) => {
		tap.test(test.name, (t) => {
			// Setup the test
			let setup = maybeRun(test.setup, client);

			const promise = setup.then(() => {
				// Run the test
				let expect = Promise.resolve([]);

				if(test.expect) {
					const handle = lq.query(test.expect.query);

					// Listen for the events
					ev.forEach((event) => {
						handle.on(event, (...data) => {
							const current = test.expect.events.shift();

							t.equal(event, current[0]);
							t.equal(JSON.stringify(data), current[1]);
						});
					});

					// Run the actions
					const queries = test.expect.actions.reduce((p, c) => {
						return p.then(() => {
							if(typeof c === 'string') {
								return helpers.query(client, c);
							}
							else {
								return new Promise((resolve, reject) => {
									setTimeout(resolve, 100);
								});
							}
						});
					}, Promise.resolve());

					expect = queries.then(() => {
						return new Promise((resolve, reject) => {
							setTimeout(() => {
								resolve([]);
							}, 500);
						});
					});
				}

				return expect.then((results) => {
					// Cleanup the test
					let cleanup = maybeRun(test.cleanup, client);

					return cleanup.then(() => {
						// End this test
						t.end();

						// Run any remaining tests
						let next = Promise.resolve([]);

						if(tests.length) {
							next = run(tests, client);
						}

						return next.then((next) => {
							return results.concat(next);
						});
					});
				});
			});

			promise.then(resolve, reject);
		});
	});
};

const pool = new Pool(test.connection);

pool.connect().then((client) => {
	run(test.tests, client).then(() => {
		client.release();
		pool.end();
	}, (e) => console.log(e));
});
