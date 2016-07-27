# Known Issues

- Queries with implicit grouping don't work (SELECT MIN(id) FROM foo)

# To Test

- Queries with CTEs
- Queries that select scalar values that change (nextval) (Should work fine, but results might be a little strange to interpret)
