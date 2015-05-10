#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPT=$(dirname "${BASH_SOURCE[0]}")
bin=${DIR}/../bin
lib=${DIR}/../lib

echo '
{
    "type" : "jdbc",
    "jdbc" : {
        "metrics" : {
            "lastexecutionstart" : "2015-05-10T10:58:00.038Z",
            "lastexecutionend" : "2015-05-10T10:58:00.044Z"
        },
        "schedule" : "0 0-59 0-23 ? * *",
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
        }
    }
}
' > ${SCRIPT}-spec.json

java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCFeeder \
< ${SCRIPT}-spec.json > ${SCRIPT}-state.json
