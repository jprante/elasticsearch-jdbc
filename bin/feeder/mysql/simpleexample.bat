@echo off

SETLOCAL

if NOT DEFINED ES_HOME goto err

set DIR=%~dp0

set FEEDER_CLASSPATH=%DIR%/*

REM ???
echo {^
    "elasticsearch" : {^
         "cluster" : "elasticsearch",^
         "host" : "localhost",^
         "port" : 9300^
    },^
    "type" : "jdbc",^
    "jdbc" : {^
        "url" : "jdbc:mysql://localhost:3306/test",^
        "user" : "",^
        "password" : "",^
        "sql" :  "select *, page_id as _id from page",^
        "treat_binary_as_string" : true,^
        "index" : "metawiki"^
      }^
}

"%JAVA_HOME%\bin\java" -cp "%FEEDER_CLASSPATH%" "org.xbib.elasticsearch.plugin.jdbc.feeder.Runner" "org.xbib.elasticsearch.plugin.jdbc.feeder.JDBCFeeder"
goto finally

:err
echo JAVA_HOME and ES_HOME environment variable must be set!
pause


:finally

ENDLOCAL