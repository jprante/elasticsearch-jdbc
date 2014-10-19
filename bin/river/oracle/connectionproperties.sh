#!/bin/sh

curl -XPUT 'localhost:9200/_river/my_oracle_river/_meta' -d '{
    "type" : "jdbc",
    "strategy" : "simple",
    "jdbc" : {
        "url" : "jdbc:oracle:thin:@//localhost:1521/hbzfl",
        "user" : "medea3",
        "password" : "homer",
        "sql" : "select or_id as \"_id\", or_tan as \"o.t\", or_status as \"o.s\", stages.* from orders, stages where or_id = st_or_id and or_seqno = st_seqno",
        "column_name_map" : {
            "o" : "order",
            "t" : "transaction_id",
            "s" : "status"
        },
        "connection_properties" : {
            "oracle.jdbc.TcpNoDelay" : false,
            "useFetchSizeWithLongColumn" : false,
            "oracle.net.CONNECT_TIMEOUT" : 10000,
            "oracle.jdbc.ReadTimeout" : 50000
        }
    }
}'
