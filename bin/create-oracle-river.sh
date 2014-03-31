
curl -XPUT 'localhost:9200/_river/my_oracle_river/_meta' -d '{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:oracle:thin:@//localhost:1521/DB",
        "user" : "user",
        "password" : "password",
        "sql" : "select * from orders"
    }
}'
