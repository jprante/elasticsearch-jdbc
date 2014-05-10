#!/bin/sh

java="/usr/bin/java"
#java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"
#java="/usr/java/jdk1.8.0/bin/java"

echo '
{
    "concurrency" : 1,
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
    "client" : "bulk",
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
' | ${java} \
    -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder
