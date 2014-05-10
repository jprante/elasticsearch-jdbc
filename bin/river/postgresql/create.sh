#!/bin/sh

curl -XPUT 'localhost:9200/_river/my_postgresql_river/_meta' -d '{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:postgresql://localhost:5432/test?loglevel=0",
        "user" : "test",
        "password" : "test",
        "sql" : "select * from large_table",
        "index_settings" : {
            "index" : {
                "number_of_shards" : 10
            }
        }
    }
}'
