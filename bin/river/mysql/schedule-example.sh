#!/bin/sh

/usr/local/mysql/bin/mysql -u root test <<EOT
drop table test;
create table test (
    id integer,
    t timestamp,
    message text
);
insert into test values (1, now(), 'Hello, this is message 1');
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
        "sql" : [
             {
               "statement": "select * from test where t > IFNULL(?,1970-01-01)",
               "parameter" : "$river.state.last_active_begin"
             }
        ],
        "index" : "mytest",
        "type" : "mydocs",
        "schedule" : "0/5 0-59 0-23 ? * *"
    }
}
'

echo "sleeping while river should run..."

sleep 5

/usr/local/mysql/bin/mysql -u root test <<EOT
insert into test values (2, now(), 'Hello, this is message 2');
EOT

sleep 5

/usr/local/mysql/bin/mysql -u root test <<EOT
insert into test values (3, now(), 'Hello, this is message 3');
EOT

sleep 5

curl -XPOST 'localhost:9200/mytest/_search?pretty' -d '
{
       "query": {
          "match_all": {
           }
       }
}'

curl -XDELETE 'localhost:9200/_river/my_test_river/'

