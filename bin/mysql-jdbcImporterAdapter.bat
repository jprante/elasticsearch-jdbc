@echo off

set DIR=%~dp0
echo "You are in path %DIR%"
set LIB=%DIR%..\lib\*
echo "LIB path %LIB%"
set BIN=%DIR%..\bin
echo "BIN path %BIN%"

"%JAVA_HOME%\bin\java" -cp "%LIB%" -Dlog4j.configurationFile="%BIN%\log4j2.xml" "org.xbib.adapter.JdbcPipelineAdapter"

