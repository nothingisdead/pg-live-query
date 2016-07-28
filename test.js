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

pool.connect((error, client, done) => {
	(function update() {
		client.query('update foo set n = n + 1 where id % 1000 = 0', (error, result) => {
			t = Date.now();
			setTimeout(update, 1000);
		});
	}());
});

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

	let sql2 = `
		SELECT
			foo.*
		FROM
			foo JOIN
			bar ON
				foo.id = bar.id
		WHERE
			foo.n < bar.n
	`;

	// // Use this to compare against just running the query
	// (function update() {
	// 	client.query(sql, (error, result) => {
	// 		if(t) {
	// 			console.log(Date.now() - t);
	// 			t = null;
	// 		}
	// 		setTimeout(update, 1);
	// 	});
	// }());

	let handler = watcher.watch(sql);

	handler
		.on('data', (rows) => {
			console.log(1, Date.now() - t, rows.length);
		})
		.on('error', (error) => {
			console.log(error);
		});

	let handler2 = watcher.watch(sql2);

	handler2
		.on('data', (rows) => {
			console.log(2, Date.now() - t, rows.length);
		})
		.on('error', (error) => {
			console.log(error);
		});
});
