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
			foo.n
		FROM
			foo
		WHERE
			id % 1000 = 0
	`;

	let handler = watcher.watch(sql);

	handler
		.on('rows', (rows) => {
			console.log(rows);
		})
		.on('error', (error) => {
			console.log(error);
		});
});
