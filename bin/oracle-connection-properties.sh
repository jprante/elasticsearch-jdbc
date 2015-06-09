#!/bin/sh

# This example is a template to connect to Oracle
# The JDBC URL and SQL must be replaced by working ones.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
bin=${DIR}/../bin
lib=${DIR}/../lib

echo '
{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:oracle:thin:@//host:1521/sid",
        "connection_properties" : {
            "oracle.jdbc.TcpNoDelay" : false,
            "useFetchSizeWithLongColumn" : false,
            "oracle.net.CONNECT_TIMEOUT" : 10000,
            "oracle.jdbc.ReadTimeout" : 50000
        },
        "user" : "user",
        "password" : "password",
        "sql" : "select or_id as \"_id\", or_tan as \"tan\" from orders",
        "index" : "myoracle",
        "type" : "myoracle",
        "elasticsearch" : {
            "cluster" : "elasticsearch",
            "host" : "localhost",
            "port" : 9300
        },
        "max_bulk_actions" : 20000,
        "max_concurrent_bulk_requests" : 10,
        "index_settings" : {
            "index" : {
                "number_of_shards" : 1,
                "number_of_replica" : 0
            }
        }
    }
}
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter
