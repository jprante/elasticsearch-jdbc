#!/bin/sh

/usr/local/mysql/bin/mysql -u root test <<EOT
drop table test if exists;
create table test (
    id integer,
    title varchar(32),
    overview varchar(64),
    test integer
);
insert into test values (0,NULL,NULL,NULL);
insert into test values (0,"Krieg der Welten","Eines windigen herbstlichen Nachmittags wird der",1212);
EOT

curl -XDELETE 'localhost:9200/_river/my_test_river/'

curl -XDELETE 'localhost:9200/mytest'

curl -XPOST 'localhost:9200/_river/my_test_river/_meta' -d '
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
'

echo "sleeping while river should run..."

sleep 10

curl -XPOST 'localhost:9200/mytest/_search?pretty' -d '
{
       "query": {
          "match_all": {
           }
       }
}'

curl -XDELETE 'localhost:9200/_river/my_test_river/'

