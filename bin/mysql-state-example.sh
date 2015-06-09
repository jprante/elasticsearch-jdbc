#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
bin=${DIR}/../bin
lib=${DIR}/../lib

echo '
{
    "type" : "jdbc",
    "jdbc" : {
        "schedule" : "0 0-59 0-23 ? * *",
        "statefile" : "statefile.json",
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "sql" : "select *, id as _id, \"myjdbc\" as _index, \"mytype\" as _type from test",
        "index" : "myjdbc",
        "type" : "mytype",
        "index_settings" : {
            "index" : {
                "number_of_shards" : 1
            }
        },
        "metrics" : {
            "enabled" : true
        }
    }
}
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter
