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
        "sql" :  "select *, page_id as _id from page where page_id < 3000000",
        "fetchsize" : "min",
        "treat_binary_as_string" : true,
        "index" : "metawiki"
    }, {
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "sql" :  "select *, page_id as _id from page where page_id >= 3000000",
        "fetchsize" : "min",
        "treat_binary_as_string" : true,
        "index" : "metawiki"
    } ]
}
' | ${JAVA_HOME}/bin/java \
    -cp ${ES_JDBC_CLASSPATH} \
    org.xbib.elasticsearch.plugin.jdbc.feeder.Runner \
    org.xbib.elasticsearch.plugin.jdbc.feeder.JDBCFeeder
