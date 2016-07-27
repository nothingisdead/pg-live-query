const Pool         = require('pg').Pool;
const QueryWatcher = require('./query_watcher');

let pool = new Pool({
	"host"     : "localhost",
	"port"     : 5432,
	"user"     : "postgres",
	"database" : "test",
	"password" : "password"
});

let t = null;

(function update() {
	t = Date.now();
	pool.connect((error, client, done) => {
		client.query('update foo set n = n + 1 where id % 1000 = 0', (error, result) => {
			done();
			setTimeout(update, 1000);
		});
	});
}());

pool.connect((error, client, done) => {
	let watcher = new QueryWatcher(client);

	let sql = `
		SELECT
			greatest(1, 2, 3),
			foo.*,
			CONCAT(concat('a', 'b'), 'bar') as blah
		FROM
			foo
		WHERE
			id % 100 = 0
	`;

	// Use this to compare against just running the query
	(function update() {
		client.query(sql, (error, result) => {
			if(t) {
				console.log(Date.now() - t);
				t = null;
			}
			setTimeout(update, 1);
		});
	}());

	// watcher.watch(sql).then((tracker) => {
	// 	(function update() {
	// 		console.time('qw');

	// 		tracker.update().then((r) => {
	// 			console.timeEnd('qw');
	// 			console.log(r.length);

	// 			setTimeout(update, 100);
	// 		}, (e) => {
	// 			console.log(e);
	// 		});
	// 	}());
	// }, (e) => {
	// 	console.log(e);
	// });
});
