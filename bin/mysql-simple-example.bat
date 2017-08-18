@echo off

set DIR=%~dp0
echo "You are in path %DIR%"
set LIB=%DIR%..\build\libs\*
echo "LIB path %LIB%"
set BIN=%DIR%..\bin
echo "BIN path %BIN%"

echo {^
    "type" : "jdbc",^
    "jdbc" : {^
        "url" : "jdbc:mysql://localhost:3306/sbes",^
        "user" : "root",^
        "password" : "",^
        "sql" :  "select * from s_movie",^
        "treat_binary_as_string" : true,^
        "elasticsearch" : {^
             "cluster" : "elasticsearch",^
             "host" : "localhost",^
             "port" : 9300^
        },^
        "index" : "s_movie"^
      }^
}^ | "%JAVA_HOME%\bin\java" -cp "%LIB%" -Dlog4j.configurationFile="%BIN%\log4j2.xml" "org.xbib.tools.Runner" "org.xbib.tools.JDBCImporter"

