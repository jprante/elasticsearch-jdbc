#!/bin/sh

curl -XPUT '0:9200/_river/my_mysql_river/_meta' -d '{
    "schedule" : "0 0-59 0-23 ? * *",
    "type" : "jdbc",
    "jdbc" : {
        "strategy" : "simple",
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "sql" : "select *, created as _id, \"myjdbc\" as _index, \"mytype\" as _type from orders",
        "index" : "myjdbc",
        "type" : "mytype",
        "index_settings" : {
            "index" : {
                "number_of_shards" : 1
            }
        }
    }
}'
