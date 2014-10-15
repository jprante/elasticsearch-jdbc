#!/bin/sh

curl -XPUT '0:9200/_river/my_postgresql_river/_meta' -d '{
    "type" : "jdbc",
    "max_bulk_actions" : 20000,
    "concurrency" : 2,
    "jdbc" : [ {
        "url" : "jdbc:postgresql://localhost:5432/test",
        "user" : "test",
        "password" : "test",
        "sql" :  "select *, page_id as _id from page where page_id < 3000000",
        "fetchsize" : "10000",
        "index" : "metawiki"
    }, {
        "url" : "jdbc:postgresql://localhost:5432/test",
        "user" : "test",
        "password" : "test",
        "sql" :  "select *, page_id as _id from page where page_id >= 3000000",
        "fetchsize" : "10000",
        "index" : "metawiki"
    } ]
}'
