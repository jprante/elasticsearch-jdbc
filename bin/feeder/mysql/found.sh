#!/bin/sh


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# ES_HOME reguired to detect elasticsearch jars
export ES_HOME=~es/elasticsearch-1.4.0.Beta1

echo '
{
    "elasticsearch" : {
         "cluster" : "elasticsearch",
         "host" : "host",
         "port" : 9300
    },
    "transport" : {
        "type" : "org.elasticsearch.transport.netty.FoundNettyTransport"
        "found" : {
            "api-key": "foobar"
        }
    },
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "sql" :  "select *, page_id as _id from page",
        "treat_binary_as_string" : true,
        "index" : "metawiki"
      }
}
' | java \
    -cp "${DIR}/*" \
    org.xbib.elasticsearch.plugin.jdbc.feeder.Runner \
    org.xbib.elasticsearch.plugin.jdbc.feeder.JDBCFeeder
