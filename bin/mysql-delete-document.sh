#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
bin=${DIR}/../bin
lib=${DIR}/../lib

echo '{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "sql" :  "select deletethisdoc as _id, delete as _optype from orders"
    }
}
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter

