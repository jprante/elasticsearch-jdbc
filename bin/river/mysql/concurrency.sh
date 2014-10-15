#!/bin/sh

curl -XPUT '0:9200/_river/my_mysql_river/_meta' -d '{
    "type" : "jdbc",
    "jdbc" : [
      {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : [
                {
                    "statement" : "select *, created as _id, \"myorders\" as _index, \"mytype\" as _type from orders"
                }
            ],
            "index" : "myorders",
            "type" : "mytype",
            "index_settings" : {
                "index" : {
                    "number_of_shards" : 1
                }
            }
      },
      {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : [
                {
                    "statement" : "select *, name as _id, \"myproducts\" as _index, \"myproducts\" as _type from products"
                }
            ],
            "index" : "myproducts",
            "type" : "mytype"
      }
    ]
}
'