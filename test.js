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
			MIN(bar.id) as bid,
			CONCAT(concat('a', 'b'), 'bar') as blah
		FROM
			foo LEFT JOIN
			bar ON
				foo.id = bar.id JOIN
			(SELECT * FROM bar WHERE id > 1) bar2 ON
				bar2.id = foo.id
		GROUP BY foo.id
	`;

	watcher.watch(sql).then((tracker) => {
		(function update() {
			tracker.update().then((r) => {
				console.log(r.map(({ data, __id__, __rev__, __op__ }) => {
					return { data, __id__, __rev__, __op__ };
				}));

				setTimeout(update, 2000);
			}, (e) => {
				console.log(e);
			});
		}());
	}, (e) => {
		console.log(e);
	});
});
