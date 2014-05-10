#!/bin/sh

java="/usr/bin/java"
#java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"
#java="/usr/java/jdk1.8.0/bin/java"

echo '
{
    "concurrency" : 2,
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
    "client" : "bulk",
    "jdbc" : [
      {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : [
                {
                    "statement" : "select *, created as _id, \"myjdbc\" as _index, \"mytype\" as _type from orders"
                }
            ],
            "index" : "myjdbc",
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
            ]
      }
    ]
}
' | ${java} \
    -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder
