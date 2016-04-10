#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
bin=${DIR}/../bin
lib=${DIR}/../lib

echo '
{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:mysql://localhost:3306/blog",
        "statefile" : "statefile.json",
        "schedule" : "0 0-59 0-23 ? * *",
        "user" : "blog",
        "password" : "12345678",
        "sql" : [{
                "statement": "select id as _id, id, post_title as title, post_content as content from wp_posts where post_status = ? and post_modified > ? ",
                "parameter": ["publish", "$metrics.lastexecutionstart"]}
            ],
        "index" : "article",
        "type" : "blog",
        "metrics": {
            "enabled" : true
        },
        "elasticsearch" : {
             "cluster" : "elasticsearch",
             "host" : "localhost",
             "port" : 9300 
        }   
    }
}
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter
