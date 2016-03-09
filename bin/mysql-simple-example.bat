@echo off

set DIR=%~dp0
set LIB=%DIR%..\lib\*
set BIN=%DIR%..\bin

REM ???
echo {^
    "type" : "jdbc",^
    "jdbc" : {^
        "url" : "jdbc:mysql://localhost:3306/test",^
        "user" : "",^
        "password" : "",^
        "sql" :  "select *, page_id as _id from page",^
        "treat_binary_as_string" : true,^
        "elasticsearch" : {^
             "cluster" : "elasticsearch",^
             "host" : "localhost",^
             "port" : 9300^
        },^
        "index" : "metawiki"^
      }^
}^ | "%JAVA_HOME%\bin\java" -cp "%LIB%" -Dlog4j.configurationFile="%BIN%\log4j2.xml" "org.xbib.tools.Runner" "org.xbib.tools.JDBCImporter"

