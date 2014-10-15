#!/bin/sh

# This example shows two concurrent feeds from a MySQL database (conncurreny = 2)
# It is possible to connect to many databases in parallel and fetch data for Elasticsearch.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. ${DIR}/../../feeder.in.sh

echo '
{
    "elasticsearch" : {
         "cluster" : "elasticsearch",
         "host" : "localhost",
         "port" : 9300
    },
    "max_bulk_actions" : 20000,
    "max_concurrent_bulk_requests" : 10,
    "concurrency" : 2,
    "type" : "jdbc",
    "jdbc" : [ {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : "statement" : "select *, created as _id, \"myorders\" as _index, \"mytype\" as _type from orders",
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
            "sql" : "statement" : "select *, name as _id, \"myproducts\" as _index, \"myproducts\" as _type from products",
            "index" : "myproducts",
            "type" : "mytype",
      }
    ]
}
' | ${JAVA_HOME}/bin/java \
    -cp ${ES_JDBC_CLASSPATH} \
    org.xbib.elasticsearch.plugin.jdbc.feeder.Runner \
    org.xbib.elasticsearch.plugin.jdbc.feeder.JDBCFeeder
