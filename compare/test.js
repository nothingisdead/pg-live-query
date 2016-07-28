const LivePg = require('pg-live-select');
const Pool   = require('pg').Pool;

let settings = {
	"host"     : "localhost",
	"port"     : 5432,
	"user"     : "postgres",
	"database" : "test",
	"password" : "password"
};

let liveDb = new LivePg(settings, 'myapp');
let pool   = new Pool(settings);

let t = null;

pool.connect((error, client, done) => {
	(function update() {
		client.query('update foo set n = n + 1 where id % 1000 = 0', (error, result) => {
			t = Date.now();
			setTimeout(update, 1000);
		});
	}());
});

liveDb.select(`
	SELECT
		greatest(1, 2, 3),
		foo.*,
		CONCAT(concat('a', 'b'), 'bar') as blah
	FROM
		foo
	WHERE
		id % 2 = 0
`).on('update', function(diff, data) {
	console.log(Date.now() - t, data.length);
});

// pool.connect((error, client, done) => {
// 	let watcher = new QueryWatcher(client);

// 	let sql = `
// 		SELECT
// 			greatest(1, 2, 3),
// 			foo.*,
// 			CONCAT(concat('a', 'b'), 'bar') as blah
// 		FROM
// 			foo
// 	`;

// 	// Use this to compare against just running the query
// 	(function update() {
// 		console.time('q');
// 		client.query(sql, (error, result) => {
// 			console.timeEnd('q');
// 			setTimeout(update, 100);
// 		});
// 	}());

// 	// watcher.watch(sql).then((tracker) => {
// 	// 	(function update() {
// 	// 		console.time('qw');

// 	// 		tracker.update().then((r) => {
// 	// 			console.timeEnd('qw');
// 	// 			console.log(r.length);

// 	// 			setTimeout(update, 100);
// 	// 		}, (e) => {
// 	// 			console.log(e);
// 	// 		});
// 	// 	}());
// 	// }, (e) => {
// 	// 	console.log(e);
// 	// });
// });
