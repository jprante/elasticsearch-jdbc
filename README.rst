.. image:: ../../../elasticsearch-river-jdbc/raw/master/src/site/resources/database-128.png

Image by `icons8 <http://www.iconsdb.com/icons8/?icon=database>`_ Creative Commons Attribution-NoDerivs 3.0 Unported.

Elasticsearch JDBC river
========================

The Java Database Connection (JDBC) `river <http://www.elasticsearch.org/guide/reference/river/>`_  allows to fetch data from JDBC sources for indexing into `Elasticsearch <http://www.elasticsearch.org>`_.

It is implemented as an `Elasticsearch plugin <http://www.elasticsearch.org/guide/reference/modules/plugins.html>`_.

The relational data is internally transformed into structured JSON objects for the schema-less indexing model in Elasticsearch.

Creating a JDBC river is easy. Install the plugin. Download a JDBC driver jar from your vendor's site (here MySQL) and put the jar into the folder of the plugin `$ES_HOME/plugins/river-jdbc`.
Then issue this simple command::

    curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
        "type" : "jdbc",
        "jdbc" : {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : "select * from orders",
            "index" : "my_jdbc_index",
            "type" : "my_jdbc_type"
        }
    }'

Installation
------------

.. image:: https://travis-ci.org/jprante/elasticsearch-river-jdbc.png

Prerequisites

  - a JDBC driver jar for your database (download from vendor site and put into JDBC river plugin folder)

=============  ===========  =================  =============================================================
ES version     Plugin       Release date       Command
-------------  -----------  -----------------  -------------------------------------------------------------
0.90.3         0.90.3.1     Jan 31, 2014       ./bin/plugin -install river-jdbc -url http://bit.ly/1emqDH9
0.90.10        0.90.10.2    Jan 31, 2014       ./bin/plugin -install river-jdbc -url http://bit.ly/1a8Mcve
1.0.0          1.0.0.2      Mar 31, 2014       ./bin/plugin --install river-jdbc --url http://bit.ly/1gIk4jW
=============  ===========  =================  =============================================================

Do not forget to restart the node after installing.

Project docs
------------

The Maven project site is available at `Github <http://jprante.github.io/elasticsearch-river-jdbc>`_

Binaries
--------

Binaries are available at `Bintray <https://bintray.com/pkg/show/general/jprante/elasticsearch-plugins/elasticsearch-river-jdbc>`_


Documentation
-------------

Attention: working on the documentation for 1.0.0.x is still in progress!

`Overview <../../../elasticsearch-river-jdbc/wiki/_pages>`_

`Quickstart <../../../elasticsearch-river-jdbc/wiki/Quickstart>`_

`JDBC river parameters <../../../elasticsearch-river-jdbc/wiki/JDBC-River-parameters>`_

`How to declare a sequence of SQL statements per river run <../../../elasticsearch-river-jdbc/wiki/Using-a-series-of-SQL-statements>`_

`Strategies <../../../elasticsearch-river-jdbc/wiki/Strategies>`_

`Moving a table <../../../elasticsearch-river-jdbc/wiki/Moving-a-table-into-Elasticsearch>`_

`Labeled columns <../../../elasticsearch-river-jdbc/wiki/Labeled-columns>`_

`Structured objects <../../../elasticsearch-river-jdbc/wiki/Structured-Objects>`_

`RiverSource, RiverMouth, RiverFlow <../../../elasticsearch-river-jdbc/wiki/RiverSource,-RiverMouth,-and-RiverFlow>`_

`Bulk indexing <../../../elasticsearch-river-jdbc/wiki/Bulk-indexing>`_

`Setting up the river with PostgreSQL <../../../elasticsearch-river-jdbc/wiki/Step-by-step-recipe-for-setting-up-the-river-with-PostgreSQL>`_

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
