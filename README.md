# bkapi

A fast way to look up hamming distance hashes.

## Querying

There are two interfaces for querying, both of which can be used simultaneously.

### HTTP

It provides a single API endpoint, `/search` which takes in a `hash` and
`distance` query parameter to search for matches. The response looks like this:

```jsonc
{
    "hash": 8525958787074004077, // Searched hash
    "distance": 3, // Maximum search distance
    "hashes": [ // Results
        {"hash": 8525958787073873005, "distance": 1}
    ]
}
```

### NATS

It listens on `$NATS_PREFIX.bkapi.search` for a payload like:

```json
[
    {"hash": 12345, "distance": 3}
]
```

Each input will have a corresponding entry in the response:

```jsonc
[
    [ // Results for first input
        {"hash": 8525958787073873005, "distance": 1}
    ]
]
```

## Initial Load

The initial entries are populated through the database query `$DATABASE_QUERY`.

## Listening

It can be configured to listen to PostgreSQL notifications OR with NATS
messages.

### PostgreSQL

It subscribes to `$DATABASE_SUBSCRIBE`, expecting events to be a JSON object
containing the hash.

### NATS

It subscribes to `$NATS_PREFIX.bkapi.add`, expecting events to be a JSON object
containing the hash.

JetStream is used to ensure no hashes are lost without having to reload the
entire tree on a connection error.
