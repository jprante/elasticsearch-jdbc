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
    "type" : "jdbc",
    "jdbc" : {
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
      }
}
' | ${JAVA_HOME}/bin/java \
    -cp ${ES_JDBC_CLASSPATH} \
    org.xbib.elasticsearch.plugin.jdbc.feeder.Runner \
    org.xbib.elasticsearch.plugin.jdbc.feeder.JDBCFeeder
