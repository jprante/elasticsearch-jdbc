#!/bin/sh

# ES 1.4.4
# plugins required: jdbc 1.4.4.0, mapper attachment 2.4.3

/usr/local/mysql/bin/mysql -u root test <<EOT
drop table test;
create table test (
    id integer,
    content blob
);
insert into test values (0,LOAD_FILE('/Users/joerg/Desktop/test.pdf'));
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
        "sql" : "select id as _id, content as \"content\" from test",
        "index" : "mytest",
        "type" : "mydocs",
        "index_settings" : { },
        "type_mapping" : {
              "mydocs" : {
                  "properties" : {
                       "content" : { 
                            "type" : "attachment",
                            "path" : "full",
                            "fields" : {
                                "content" : {
                                    "type" : "string",
                                    "store" : true
                                }
                            }
                       }
                  }
              }
        }
    }
}
'

echo "sleeping while river should run..."

sleep 5

curl -XGET 'localhost:9200/mytest/_mapping?pretty'

curl -XPOST 'localhost:9200/mytest/_search?pretty' -d '
{
       "fields" : [ "content" ],
       "query": {
          "match": {
               "content" : "Medea"
           }
       }
}'

curl -XDELETE 'localhost:9200/_river/my_test_river/'

