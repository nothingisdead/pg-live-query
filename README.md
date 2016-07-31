# pg-live-query

This package makes it possible in PostgreSQL to get (almost) realtime notifications whenever the results of a query change.

## Usage

To use this package, you need to also use the amazing [pg](https://www.npmjs.com/package/pg) driver for PostgreSQL.

```javascript
const Pool      = require('pg').Pool;
const LiveQuery = require('pg-live-query');

const pool = new Pool(); // Or however you set up pg

pool.connect((error, client, done) => {
    const lq = new LiveQuery(client);

    const sql = `
        SELECT
            *
        FROM
            users u JOIN
            logins l ON
                l.user_id = u.id
        WHERE
            l.date > '2016-01-01'
    `;

    // To get 'insert', 'update', 'delete', and 'changes' events
    const handle = lq.watch(sql);
    // To get the above plus an additional 'rows' event with the full rowset
    // This consumes more memory as it has to maintain the current state of the rowset
    const handle = lq.query(sql);

    // The "update" event looks the same as "insert"
    // They contain the id, row data (as an array), and column names
    handle.on('insert', (id, row, cols) => {
        const out = {};

        cols.forEach((col, i) => {
            out[col] = row[i] || null;
        });

        console.log('row inserted', id, row);
    });

    // The "delete" event only contains the id
    handle.on('delete', (id) => {
        console.log('row deleted', id);
    });

    // The "changes" event contains several changes batched together
    handle.on('changes', (changes) => {
        changes.forEach(({ id, rn, data }) => {
            if(data) {
                console.log(`upsert: ${id} at row ${rn}`, data);
            }
            else {
                console.log(`delete: ${id}`);
            }
        });
    });

    // The "rows" event contains an array of objects
    // that represent the entire current result set
    handle.on('rows', (rows) => {
        console.log('all rows', rows);
    });
});
```

## How it Works

This package should provide much better performance than it used to. *I definitely wouldn't consider it production-ready*, but that's at least a feasible goal now.

The way it works is by computing an aggregate unique id and latest revision based on columns (that this package adds) from each input row that contributed to a particular output row. It also stores the previous state of the result set in a temporary table. This makes it trivial to compute the differences between two result sets from different executions of the same query inside the database, instead of in Node.

## Known Issues

- Queries with implicit grouping don't work (SELECT MIN(id) FROM foo)

## To Test

- Queries with CTEs
- Queries with UNIONs
- Queries that select scalar values that change each time the query is run (nextval, current_timestamp)

*Note: This package was previously "deprecated" in favor of pg-live-select. It has since been completely rewritten with a fundamentally different mechanism for determining changes. It should greatly outperform both pg-live-select and any previous versions of this package.*
