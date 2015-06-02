#!/bin/sh

# Schedule with acknowledging internal flags back to an SQL table

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
        "schedule": "0 0-59 0-23 ? * *",
        "sql" : [ {
                "statement" : "select *, id as _id, \"myjdbc\" as _index, \"mytype\" as _type from test.test"
            },
            {
                "statement": "insert into test.ack(n,t,c,s,e,ex,ext) values(?,?,?,?,?,?,?)",
                    "parameter": [
                        "$job",
                        "$now",
                        "$metrics.totalrows",
                        "$metrics.lastexecutionstart",
                        "$metrics.lastexecutionend",
                        "$lastexception",
                        "$lastexceptiondate"
                    ]
            }
        ],
        "autocommit": true,
        "elasticsearch" : {
            "cluster" : "elasticsearch",
            "host" : "localhost",
            "port" : 9300
        },
        "metrics" : {
            "enabled" : true,
            "interval" : "10s"
        },
        "state" : "schedule.json"
      }
}
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter