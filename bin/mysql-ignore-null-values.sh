#!/bin/sh

/usr/local/mysql/bin/mysql -u root test <<EOT
drop table test.test;
create table test.test (
    id integer,
    title varchar(32),
    overview varchar(64),
    test integer
);
insert into test.test values (0,NULL,NULL,NULL);
insert into test.test values (0,"Krieg der Welten","Eines windigen herbstlichen Nachmittags wird der",1212);
EOT

curl -XDELETE 'localhost:9200/mytest'

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
        "sql" : "select id as _id, title as \"movie.title\", overview as \"movie.overview\", test as \"movie.test\" from test",
        "ignore_null_values" : true,
        "index" : "mytest"
    }
}
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter


echo "sleeping while river should run..."

sleep 10

curl -XPOST 'localhost:9200/mytest/_search?pretty' -d '
{
       "query": {
          "match_all": {
           }
       }
}'

# expected

#{
#  "took" : 15,
#  "timed_out" : false,
#  "_shards" : {
#    "total" : 5,
#    "successful" : 5,
#    "failed" : 0
#  },
#  "hits" : {
#    "total" : 1,
#    "max_score" : 1.0,
#    "hits" : [ {
#      "_index" : "mytest",
#      "_type" : "jdbc",
#      "_id" : "0",
#      "_score" : 1.0,
#      "_source":{"movie":{"title":"Krieg der Welten","overview":"Eines windigen herbstlichen Nachmittags wird der","test":1212}}
#    } ]
#  }
#}
