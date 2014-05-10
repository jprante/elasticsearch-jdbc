#!/bin/sh

java="/usr/bin/java"
#java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"
#java="/usr/java/jdk1.8.0/bin/java"

echo '
{
    "concurrency" : 1,
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
    "client" : "ingest",
    "index" : "myh2",
    "type" : "myh2",
    "jdbc" : [
      {
        "url" : "jdbc:h2:test",
        "user" : "",
        "password" : "",
        "sql" : [
          {
            "statement" : "select *, created as _id, \"myjdbc\" as _index, \"mytype\" as _type from \"orders\""
          }
        ]
      }
    ]
}
' | ${java} \
    -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder
