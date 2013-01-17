
curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
    "type" : "jdbc",
    "jdbc" : {
        "driver" : "org.xbib.jdbc.csv.CsvDriver",
        "url" : "jdbc:csv:plugins/river-jdbc/datadir?columnTypes=&separator=;",
        "user" : "",
        "password" : "",
        "sql" : "select * from Gesamtfinanzplan"
    }
}'
