#!/bin/sh

# Index a sql dump from Wikipedia
# 1. Download from http://meta.wikimedia.org/wiki/Data_dumps
# 2. Load into MySQL
# 3. Start ES
# 4. Run this script

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
        "treat_binary_as_string" : true,
        "elasticsearch" : {
            "cluster" : "elasticsearch",
            "host" : "localhost",
            "port" : 9300
        },
        "max_bulk_actions" : 20000,
        "max_concurrent_bulk_requests" : 10,
        "index" : "metawiki"
      }
}
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter
