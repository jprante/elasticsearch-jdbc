.. image:: ../../../elasticsearch-river-jdbc/raw/master/src/site/origami.png

Elasticsearch JDBC river
========================

The Java Database Connection (JDBC) `river <http://www.elasticsearch.org/guide/reference/river/>`_  allows to fetch data from JDBC sources for indexing into `Elasticsearch <http://www.elasticsearch.org>`_. 

It is implemented as an `Elasticsearch plugin <http://www.elasticsearch.org/guide/reference/modules/plugins.html>`_.

The relational data is internally transformed into structured JSON objects for the schema-less indexing model in Elasticsearch. 

Creating a JDBC river is easy::

    curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
        "type" : "jdbc",
        "jdbc" : {
            "driver" : "com.mysql.jdbc.Driver",
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : "select * from orders"
        }
    }'

Installation
------------

Prerequisites::

  Elasticsearch 0.20+ / 0.90.0.Beta1+
  a JDBC driver jar of your database

=============  =========  =================  ============================================================
ES version     Plugin     Release date       Command
-------------  ---------  -----------------  ------------------------------------------------------------
0.20+          **2.0.3**  February 12, 2013  ./bin/plugin -url http://bit.ly/Yp2Drj -install river-jdbc
0.90.0.Beta1+  **2.2.0**  February 28, 2013  ./bin/plugin -url http://bit.ly/145e9Ly -install river-jdbc
=============  =========  =================  ============================================================

Bintray:

https://bintray.com/pkg/show/general/jprante/elasticsearch-plugins/elasticsearch-river-jdbc

Documentation
-------------

`Quickstart <../../../elasticsearch-river-jdbc/wiki/Quickstart>`_

`JDBC river parameters <../../../elasticsearch-river-jdbc/wiki/JDBC-River-parameters>`_

`Strategies <../../../elasticsearch-river-jdbc/wiki/Strategies>`_

`Moving a table <../../../elasticsearch-river-jdbc/wiki/Moving-a-table-into-Elasticsearch>`_

`Labeled columns <../../../elasticsearch-river-jdbc/wiki/Labeled-columns>`_

`Structured objects <../../../elasticsearch-river-jdbc/wiki/Structured-Objects>`_

`RiverSource, RiverMouth, RiverFlow <../../../elasticsearch-river-jdbc/wiki/RiverSource,-RiverMouth,-and-RiverFlow>`_

`Bulk indexing <../../../elasticsearch-river-jdbc/wiki/Bulk-indexing>`_

`Updates with versioning <../../../elasticsearch-river-jdbc/wiki/Updates-with-versioning>`_

`Updates with database table <../../../elasticsearch-river-jdbc/wiki/Updates-with-database-table>`_

`Setting up the river with PostgreSQL <../../../elasticsearch-river-jdbc/wiki/Step-by-step-recipe-for-setting-up-the-river-with-PostgreSQL>`_

`Loading from CSV <../../../elasticsearch-river-jdbc/wiki/Loading-CSV>`_

License
=======

Elasticsearch JDBC River Plugin

Copyright (C) 2012,2013 Jörg Prante

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.