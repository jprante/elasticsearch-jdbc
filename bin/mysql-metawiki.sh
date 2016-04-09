#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
bin=${DIR}/../bin
lib=${DIR}/../lib

echo '
{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "sql" :  "select *, page_id as _id from page",
        "fetchsize" : "min",
        "treat_binary_as_string" : true,
        "max_bulk_actions" : 20000,
        "max_concurrent_bulk_requests" : 10,
        "metrics" : {
            "enabled" : true,
            "logger" : {
                "plain" : true,
                "json" : true
            },
            "interval" : "1s"
        }
    }
}
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter
