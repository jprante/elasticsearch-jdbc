#!/bin/sh

# add Oracle JDBC driver jar to lib folder
# setup Oracle table:
#   create table test ( t timestamp, str varchar2(32) )
# start this script, then
#   insert into test (t,str) values(sysdate, 'Hello One')
# after another minute, then
#   insert into test (t,str) values(sysdate, 'Hello Two')
#
# file schedule-oracle.json contains current state

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
bin=${DIR}/../bin
lib=${DIR}/../lib

echo '
{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:oracle:thin:@//localhost:1521/sid",
        "user" : "user",
        "password" : "password",
        "sql" : {
           "statement" : "select str \"_id\", '\''myjdbc'\'' \"_index\", '\''mytype'\'' \"_type\" from test where t > ?",
           "parameter" : [ "$metrics.lastexecutionstart" ]
        },
        "index" : "myjdbc",
        "type" : "mytype",
        "index_settings" : {
            "index" : {
                "number_of_shards" : 1
            }
        },
        "metrics" : {
            "enabled" : true,
            "lastexecutionstart" : "2015-10-10T10:58:00.038Z",
            "lastexecutionend" : "2015-10-10T10:58:00.044Z"
        },
        "statefile" : "schedule-oracle.json",
        "schedule" : "0 0-59 0-23 ? * *"
    }
}
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter
