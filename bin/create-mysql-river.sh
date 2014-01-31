
curl -XPUT '0:9200/_river/my_jdbc_river/_meta' -d '{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "schedule" : "0 0-59 0-23 ? * *",
        "sql" : [
            {
                "statement" : "select *, created as _id, \"myjdbc\" as _index, \"mytype\" as _type from orders"
            }
        ]
    }
}'
