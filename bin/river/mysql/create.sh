#!/bin/sh

curl -XPUT '0:9200/_river/my_mysql_river/_meta' -d '{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "sql" :  "select *, created as _id from orders"
    }
}'
