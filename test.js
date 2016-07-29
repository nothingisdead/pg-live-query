const Pool         = require('pg').Pool;
const QueryWatcher = require('./query_watcher');

let pool = new Pool({
	"host"     : "localhost",
	"port"     : 5432,
	"user"     : "postgres",
	"database" : "test",
	"password" : "password"
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
			id % 2 = 0
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

	let t  = null;
	let t2 = null;

	let handler = watcher.watch(sql);

	handler
		.on('rows', (rows) => {
			if(t) {
				console.log(1, Date.now() - t, rows.length);
				t = null;
			}
		})
		.on('error', (error) => {
			console.log(error);
		});

	let handler2 = watcher.watch(sql2);

	handler2
		.on('rows', (rows) => {
			if(t2) {
				console.log(2, Date.now() - t2, rows.length);
				t2 = null;
			}
		})
		.on('error', (error) => {
			console.log(error);
		});

	pool.connect((error, client, done) => {
		(function update() {
			client.query('update foo set n = n + 1 where id % 1000 = 0', (error, result) => {
				if(!t) {
					t = Date.now();
				}

				if(!t2) {
					t2 = Date.now();
				}

				setTimeout(update, 1000);
			});
		}());
	});
});
