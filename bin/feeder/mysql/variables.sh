#!/bin/sh

# ES_HOME reguired to detect elasticsearch jars
export ES_HOME=~es/elasticsearch-1.4.4

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
        "sql" :  [
             {
               "statement": "select round(count(*) / 10000) as num from page"
             },
             {
               "statement": "select *, page_id as _id from page limit ?",
               "parameter" : "$row.num"
             }
         ],
        "treat_binary_as_string" : true,
        "index" : "metawiki"
      }
}
' | java \
    -cp "${ES_HOME}/lib/*:${ES_HOME}/plugins/jdbc/*" \
    org.xbib.elasticsearch.plugin.jdbc.feeder.Runner \
    org.xbib.elasticsearch.plugin.jdbc.feeder.JDBCFeeder
