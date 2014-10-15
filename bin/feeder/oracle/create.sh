#!/bin/sh

# This example is a template to connect to Oracle in feeder mode.
# The JDBC URL and SQL must be replaced by working ones.

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
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:oracle:thin:@//host:1521/sid",
        "user" : "user",
        "password" : "password",
        "sql" : "select or_id as \"_id\", or_tan as \"tan\" from orders",
        "index" : "myoracle",
        "type" : "myoracle",
        "index_settings" : {
            "index" : {
                "number_of_shards" : 1,
                "number_of_replica" : 0
            }
        }
    }
}
' | ${JAVA_HOME}/bin/java \
    -cp ${ES_JDBC_CLASSPATH} \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder
