#!/bin/sh

# This example shows a complete geo data push & search example
# for Postgresql -> Elasticsearch

# - install Elasticsearch
# - run Elasticsearch
# - install Postgresql
# - start Postgresql
# - modifiy line with PSQL in this script
# - run this script

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
bin=${DIR}/../bin
lib=${DIR}/../lib

PSQL="sudo -u postgres /Library/PostgreSQL/9.3/bin/psql"

${PSQL} -d test -U test <<EOF
drop table if exists geo;
create table geo (
  lat float,
  lon float,
  id integer,
  zip varchar(255),
  name varchar(255),
  address varchar(255),
  city varchar(255)
);
insert into geo values (50.9406645,6.9599115,NULL,'50667','Dom','Domkloster 4','Köln');
EOF

curl -XDELETE 'localhost:9200/myjdbc'

echo '
{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:postgresql://localhost:5432/test",
        "user" : "test",
        "password" : "test",
        "locale" : "en_US",
        "sql" : "select name as _id, city, zip, address, lat as \"location.lat\", lon as \"location.lon\" from geo",
        "elasticsearch" : {
             "cluster" : "elasticsearch",
             "host" : "localhost",
             "port" : 9300
        },
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
' | java \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter

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
