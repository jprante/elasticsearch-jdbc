.. image:: elasticsearch-river-jdbc/raw/master/src/site/origami.png

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

Prerequisite: Elasticsearch **0.20.x**

The current version of the plugin is **2.0.0**

Download:

Documentation
-------------

`<wiki/Quickstart>`_

`<wiki/Strategies>`_

`<wiki/Moving-a-table-into-Elasticsearch>`_

`<wiki/Labeled-columns>`_

`<wiki/Structured-Objects>`_

`<wiki/RiverSource,-RiverMouth,-and-RiverFlow>`_

`<wiki/Bulk-indexing>`_

`<wiki/Step-by-step-recipe-for-setting-up-the-river-with-PostgreSQL>`_

`<wiki/Updates-with-versioning>`_

`<wiki/Updates-with-database-table>`_

`<wiki/Loading-CSV>`_

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