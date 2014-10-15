#!/bin/sh

# This example shows a complete minimalistic geo push & search example for MySQL -> Elasticsearch

# - install Elasticsearch
# - run Elasticsearch
# - install MySQL in /usr/local/mysql
# - start MySQL on localhost:3306
# - as MySQL root admin, prepare a 'geo' database in MySQL :
#     CREATE DATABASE geo
# - as MySQL root admin, create empty user '' with empty password '' :
#     GRANT ALL PRIVILEGES ON geo.* TO ''@'localhost' IDENTIFIED BY '';
# - execute SQL in geo.dump
#     /usr/local/mysql/bin/mysql geo < ./bin/feeder/mysql/geo.dump
# - then run this script
#    ./bin/feeder/mysql/geo.sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. ${DIR}/../../feeder.in.sh

curl -XDELETE 'localhost:9200/myjdbc'

echo '
{
    "elasticsearch" : {
         "cluster" : "elasticsearch",
         "host" : "localhost",
         "port" : 9300
    },
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "locale" : "en_US",
        "sql" : "select \"myjdbc\" as _index, \"mytype\" as _type, name as _id, city, zip, address, lat as \"location.lat\", lon as \"location.lon\" from geo",
        "index" : "myjdbc",
        "type" : "mytype",
        "index_settings" : {
            "index" : {
                "number_of_shards" : 1
            }
        },
        "type_mapping": {
            "mytype" : {
                "properties" : {
                    "location" : {
                        "type" : "geo_point"
                    }
                }
            }
        }
    }
}
' | ${JAVA_HOME}/bin/java \
    -cp ${ES_JDBC_CLASSPATH} \
    org.xbib.elasticsearch.plugin.jdbc.feeder.Runner \
    org.xbib.elasticsearch.plugin.jdbc.feeder.JDBCFeeder

curl -XGET 'localhost:9200/myjdbc/_refresh'

curl -XPOST 'localhost:9200/myjdbc/_search?pretty' -d '
{
  "query": {
     "filtered": {
       "query": {
          "match_all": {
           }
       },
       "filter": {
           "geo_distance" : {
               "distance" : "20km",
               "location" : {
                    "lat" : 51.0,
                    "lon" : 7.0
                }
            }
        }
     }
   }
}'

# Expected result:
# {"_shards":{"total":2,"successful":1,"failed":0}}{
#  "took" : 117,
#  "timed_out" : false,
#  "_shards" : {
#    "total" : 1,
#    "successful" : 1,
#    "failed" : 0
#  },
#  "hits" : {
#    "total" : 1,
#    "max_score" : 1.0,
#    "hits" : [ {
#      "_index" : "myjdbc",
#      "_type" : "mytype",
#      "_id" : "Dom",
#      "_score" : 1.0,
#      "_source":{"city":"Köln","zip":"50667","address":"Domkloster 4","location":{"lat":50.9406645,"lon":6.9599115}}
#    } ]
#  }
# }
