#!/bin/sh

# Polygon geo shape demo

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
bin=${DIR}/../bin
lib=${DIR}/../lib

/usr/local/mysql/bin/mysql -u root test <<EOT
drop table if exists test.geom;
create table test.geom (
    id integer,
    g geometry
);
set @g = 'POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))';
insert into test.geom values (0, GeomFromText(@g));
EOT

curl -XDELETE 'localhost:9200/myjdbc'

echo '
{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:mysql://localhost:3306/test",
        "user" : "",
        "password" : "",
        "locale" : "en_US",
        "sql" : "select \"myjdbc\" as _index, \"mytype\" as _type, id as _id, astext(g) as polygon from geom",
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
                    "polygon" : {
                        "type" : "geo_shape",
                        "tree" : "quadtree"
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

curl -XPOST 'localhost:9200/myjdbc/_search?pretty' -d '
{
    "query":{
        "filtered": {
            "query": {
                "match_all": {}
            },
            "filter": {
                "geo_shape": {
                    "polygon": {
                        "shape": {
                            "type": "envelope",
                            "coordinates" : [[1.0, 5.0], [2.0, 6.0]]
                        }
                    }
                }
            }
        }
    }
}
'

# expected:
#{
#  "took" : 11,
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
#      "_id" : "0",
#      "_score" : 1.0,
#      "_source":{"polygon":{"coordinates":[[[0.0,0.0],[10.0,0.0],[10.0,10.0],[0.0,10.0],[0.0,0.0]],[[5.0,5.0],[7.0,5.0],[7.0,7.0],[5.0,7.0],[5.0,5.0]]],"type":"Polygon"}}
#    } ]
#  }
#}

