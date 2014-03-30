
curl -XPUT 'localhost:9200/_river/my_oracle_river/_meta' -d '{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:oracle:thin:@//dionysos.hbz-nrw.de:1521/ill",
        "user" : "medea3",
        "password" : "homer",
        "sql" : "select * from institutions"
    }
}'
