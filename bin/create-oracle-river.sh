
curl -XPUT 'localhost:9200/_river/my_medea_river/_meta' -d '{
    "type" : "jdbc",
    "jdbc" : {
        "driver" : "oracle.jdbc.driver.OracleDriver",
        "url" : "jdbc:oracle:thin:@//harmonia.hbz-nrw.de:1521/hbzfl",
        "user" : "medea3",
        "password" : "homer",
        "sql" : "select * from orders"
    },
    "index" : {
        "index" : "medea",
        "type" : "orders"
    }
}'
