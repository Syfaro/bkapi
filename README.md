# bkapi

A fast way to look up hamming distance hashes.

It operates by connecting to a PostgreSQL database (`DATABASE_URL`), selecting
every column of a provided query (`DATABASE_QUERY`), subscribing to events
(`DATABASE_SUBSCRIBE`), and holding everything in a BK tree.

It provides a single API endpoint, `/search` which takes in a `hash` and
`distance` query parameter to search for matches.
