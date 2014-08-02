
# Configuration for Elasticsearch JDBC feeder mechanism

# Java home

# for Mac OS X
#JAVA_HOME=$(/usr/libexec/java_home -v 1.7*)
JAVA_HOME=$(/usr/libexec/java_home -v 1.8*)
# for Linux
#JAVA_HOME"/etc/alternatives/java"

# Elasticsearch home
ES_HOME=~es/elasticsearch-1.3.0

# Elasticsearch plugins folder where "jdbc" plugin is installed
ES_PATH_PLUGINS=${ES_HOME}/plugins

# Classpath for loading JDBC plugin from external Java execution, without other plugins.
#
# The classpath is very similar to Elasticsearch classpath, but it must follow these rules:
# - first, the elasticsearch*.jar in elasticsearch "lib" folder
# - the other jars in elasticsearch "lib" folder
# - the plugins/jdbc folder for log4j.properties (or log4j2.xml)
# - the plugins/jdbc jars (plugin jar and JDBC driver jars)
# - no more, no other (server-side) plugins etc. !
ES_JDBC_CLASSPATH=${ES_HOME}/lib/elasticsearch\*:${ES_HOME}/lib/\*:${ES_PATH_PLUGINS}/jdbc:${ES_PATH_PLUGINS}/jdbc/\*
