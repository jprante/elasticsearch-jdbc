FROM java:8-jre

# Install base packages
RUN apt-get -y update \
 && apt-get install -y unzip wget

# Install dockerize
RUN wget -O /tmp/dockerize-linux-amd64-v0.0.4.tar.gz https://github.com/jwilder/dockerize/releases/download/v0.0.4/dockerize-linux-amd64-v0.0.4.tar.gz \
 && tar -C /usr/local/bin -xzvf /tmp/dockerize-linux-amd64-v0.0.4.tar.gz

# Install elasticsearch-jdbc
RUN wget -O /tmp/elasticsearch-jdbc-1.7.3.0.zip  http://xbib.org/repository/org/xbib/elasticsearch/importer/elasticsearch-jdbc/1.7.3.0/elasticsearch-jdbc-1.7.3.0-dist.zip \
 && unzip -d /opt /tmp/elasticsearch-jdbc-1.7.3.0.zip \
 && ln -s /opt/elasticsearch-jdbc-1.7.3.0 /opt/elasticsearch-jdbc

# Get the jdbc driver for postgresql 9.4
RUN wget -O /opt/elasticsearch-jdbc-1.7.3.0/lib/postgresql-9.4-1205.jdbc42.jar https://jdbc.postgresql.org/download/postgresql-9.4-1205.jdbc42.jar

# Touch log file
RUN mkdir -p /opt/elasticsearch-jdbc/logs \
 && touch /opt/elasticsearch-jdbc/logs/jdbc.log

# Clean up APT when done.
RUN apt-get remove -y unzip wget \
 && apt-get autoremove -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ADD config.json /

WORKDIR /opt/elasticsearch-jdbc

CMD dockerize \
    -template /config.json:/tmp/config.json \
    -stdout /opt/elasticsearch-jdbc/logs/jdbc.log \
    -stdout /statefile.json \
     java \
    -cp "/opt/elasticsearch-jdbc/lib/*" \
    -Dlog4j.configurationFile=/opt/elasticsearch-jdbc/bin/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.JDBCImporter \
    /tmp/config.json



