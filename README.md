JDBC River Plugin for ElasticSearch
===================================

Introduction
------------

The Java Database Connection (JDBC) [river](http://www.elasticsearch.org/guide/reference/river/) 
allows to select data from JDBC sources for indexing into ElasticSearch. 

It is implemented as an [Elasticsearch plugin](http://www.elasticsearch.org/guide/reference/modules/plugins.html).

The relational data is internally transformed into structured JSON objects for 
ElasticSearch schema-less indexing. 

Setting it up is as simple as executing something like the following against 
ElasticSearch:

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "driver" : "com.mysql.jdbc.Driver",
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select * from orders",
	    }
	}'

This HTTP PUT statement will create a river named `my_jdbc_river` 
that fetches all the rows from the `orders` table in the MySQL database 
`test` at `localhost`.

You have to install the JDBC driver jar of your favorite database manually into 
the `plugins` directory where the jar file of the JDBC river plugin resides.

By default, the JDBC river re-executes the SQL statement on a regular basis (60 minutes).

In case of a failover, the JDBC river will automatically be restarted 
on another ElasticSearch node, and continue indexing.

Many JDBC rivers can run in parallel. Each river opens one thread to select
the data.

Installation
------------

In order to install the plugin, simply run: `bin/plugin -install jprante/elasticsearch-river-jdbc/1.0.1`.

    -------------------------------------
    | JDBC Plugin    | ElasticSearch    |
    -------------------------------------
    | master         | 0.19.3 -> master |
    -------------------------------------
    | 1.0.1          | 0.19.3           |
    -------------------------------------

Documentation
-------------

The Maven project site is [here](http://jprante.github.com/elasticsearch-river-jdbc)

The Javadoc API can be found [here](http://jprante.github.com/elasticsearch-river-jdbc/apidocs/index.html)

Log example of river creation
-----------------------------
	[2012-06-16 18:50:10,035][INFO ][cluster.metadata         ] [Anomaly] [_river] update_mapping [my_jdbc_river] (dynamic)
	[2012-06-16 18:50:10,046][INFO ][river.jdbc               ] [Anomaly] [jdbc][my_jdbc_river] starting JDBC connector: URL [jdbc:mysql://localhost:3306/test], driver [com.mysql.jdbc.Driver], sql 	[select * from orders], indexing to [jdbc]/[jdbc], poll [1h]
	[2012-06-16 18:50:10,129][INFO ][cluster.metadata         ] [Anomaly] [jdbc] creating index, cause [api], shards [5]/[1], mappings []
	[2012-06-16 18:50:10,353][INFO ][cluster.metadata         ] [Anomaly] [_river] update_mapping [my_jdbc_river] (dynamic)
	[2012-06-16 18:50:10,714][INFO ][river.jdbc               ] [Anomaly] [jdbc][my_jdbc_river] got 5 rows
	[2012-06-16 18:50:10,719][INFO ][river.jdbc               ] [Anomaly] [jdbc][my_jdbc_river] next run, waiting 1h, URL [jdbc:mysql://localhost:3306/test] driver [com.mysql.jdbc.Driver] sql [select * from orders]

Configuration
=============

The SQL statements used for selecting can be configured as follows.

Star query
----------

Star queries are the simplest variant of selecting data. They can be used
to dump tables into ElasticSearch. 

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

For example

	mysql> select * from orders;
	+----------+-----------------+---------+----------+---------------------+
	| customer | department      | product | quantity | created             |
	+----------+-----------------+---------+----------+---------------------+
	| Big      | American Fruits | Apples  |        1 | 0000-00-00 00:00:00 |
	| Large    | German Fruits   | Bananas |        1 | 0000-00-00 00:00:00 |
	| Huge     | German Fruits   | Oranges |        2 | 0000-00-00 00:00:00 |
	| Good     | German Fruits   | Apples  |        2 | 2012-06-01 00:00:00 |
	| Bad      | English Fruits  | Oranges |        3 | 2012-06-01 00:00:00 |
	+----------+-----------------+---------+----------+---------------------+
	5 rows in set (0.00 sec)

The JSON objects are flat, the `id`
of the documents is generated automatically, it is the row number.

	id=0 {"product":"Apples","created":null,"department":"American Fruits","quantity":1,"customer":"Big"}
	id=1 {"product":"Bananas","created":null,"department":"German Fruits","quantity":1,"customer":"Large"}
	id=2 {"product":"Oranges","created":null,"department":"German Fruits","quantity":2,"customer":"Huge"}
	id=3 {"product":"Apples","created":1338501600000,"department":"German Fruits","quantity":2,"customer":"Good"}
	id=4 {"product":"Oranges","created":1338501600000,"department":"English Fruits","quantity":3,"customer":"Bad"}

Labeled columns
---------------

In SQL, each column may be labeled with a name. This name is used by the JDBC river to JSON object construction.

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "driver" : "com.mysql.jdbc.Driver",
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product"
	    }
	}'

In this query, the columns selected are described as `product.name`, 
`product.customer.name`, and `product.customer.bill`.

	mysql> select products.name as "product.name", orders.customer as "product.customer", orders.quantity * products.price as "product.customer.bill" from products, orders where products.name = orders.product ;
	+--------------+------------------+-----------------------+
	| product.name | product.customer | product.customer.bill |
	+--------------+------------------+-----------------------+
	| Apples       | Big              |                     1 |
	| Bananas      | Large            |                     2 |
	| Oranges      | Huge             |                     6 |
	| Apples       | Good             |                     2 |
	| Oranges      | Bad              |                     9 |
	+--------------+------------------+-----------------------+
	5 rows in set, 5 warnings (0.00 sec)

The JSON objects are

	id=0 {"product":{"name":"Apples","customer":{"bill":1.0,"name":"Big"}}}
	id=1 {"product":{"name":"Bananas","customer":{"bill":2.0,"name":"Large"}}}
	id=2 {"product":{"name":"Oranges","customer":{"bill":6.0,"name":"Huge"}}}
	id=3 {"product":{"name":"Apples","customer":{"bill":2.0,"name":"Good"}}}
	id=4 {"product":{"name":"Oranges","customer":{"bill":9.0,"name":"Bad"}}}

There are three column labels with an underscore as prefix 
that are mapped to the Elasticsearch index/type/id.

	_id
	_type
	_index

Structured objects
------------------

One of the advantage of SQL queries is the join operation. From many tables,
new tuples can be formed.

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "driver" : "com.mysql.jdbc.Driver",
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select \"relations\" as \"_index\", orders.customer as \"_id\", orders.customer as \"contact.customer\", employees.name as \"contact.employee\" from orders left join employees on employees.department = orders.department"
	    }
	}'

For example, these rows from SQL

	mysql> select "relations" as "_index", orders.customer as "_id", orders.customer as "contact.customer", employees.name as "contact.employee"  from orders left join employees on employees.department = orders.department;
	+-----------+-------+------------------+------------------+
	| _index    | _id   | contact.customer | contact.employee |
	+-----------+-------+------------------+------------------+
	| relations | Big   | Big              | Smith            |
	| relations | Large | Large            | MŸller           |
	| relations | Large | Large            | Meier            |
	| relations | Large | Large            | Schulze          |
	| relations | Huge  | Huge             | MŸller           |
	| relations | Huge  | Huge             | Meier            |
	| relations | Huge  | Huge             | Schulze          |
	| relations | Good  | Good             | MŸller           |
	| relations | Good  | Good             | Meier            |
	| relations | Good  | Good             | Schulze          |
	| relations | Bad   | Bad              | Jones            |
	+-----------+-------+------------------+------------------+
	11 rows in set (0.00 sec)

will generate fewer JSON objects for the index `relations`.

	index=relations id=Big {"contact":{"employee":"Smith","customer":"Big"}}
	index=relations id=Large {"contact":{"employee":["MŸller","Meier","Schulze"],"customer":"Large"}}
	index=relations id=Huge {"contact":{"employee":["MŸller","Meier","Schulze"],"customer":"Huge"}}
	index=relations id=Good {"contact":{"employee":["MŸller","Meier","Schulze"],"customer":"Good"}}
	index=relations id=Bad {"contact":{"employee":"Jones","customer":"Bad"}}

Note how the `employee` column is collapsed into a JSON array. The repeated occurence of the `_id` column
controls how values are folded into arrays for making use of the ElasticSearch JSON data model.

Bind parameter
--------------

Bind parameters are useful for selecting rows according to a matching condition
where the match criteria is not known beforehand.

For example, only rows matching certain conditions can be indexed into
ElasticSearch.

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "driver" : "com.mysql.jdbc.Driver",
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product and orders.quantity * products.price > ?",
	        "params: [ 5.0 ]
	    }
	}'

Example result

	id=0 {"product":{"name":"Oranges","customer":{"bill":6.0,"name":"Huge"}}}
	id=1 {"product":{"name":"Oranges","customer":{"bill":9.0,"name":"Bad"}}}


Time-based selecting
--------------------

Because the JDBC river is running repeatedly, time-based selecting is useful.
The current time is represented by the parameter value `$now`.

In this example, all rows beginning with a certain date up to now are selected.

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "driver" : "com.mysql.jdbc.Driver",
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product and orders.created between ? - 14 and ?",
	        "params: [ 2012-06-01", "$now" ]
	    }
	}'

Example result:

	id=0 {"product":{"name":"Apples","customer":{"bill":2.0,"name":"Good"}}}
	id=1 {"product":{"name":"Oranges","customer":{"bill":9.0,"name":"Bad"}}}

Index
-----

Each river can index into a specified index. Example:

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "driver" : "com.mysql.jdbc.Driver",
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select * from orders",
	    },
	    "index" : {
	        "index" : "jdbc",
	        "type" : "jdbc"
	    }
	}'

Bulk indexing
-------------

Bulk indexing is automatically used in order to speed up the indexing process. 

Each SQL result set will be indexed by a single bulk if the bulk size is not specified.

A bulk size can be defined, also a maximum size of active bulk requests to cope with high load situations.
A bulk timeout defines the time period after which bulk feeds continue.

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "driver" : "com.mysql.jdbc.Driver",
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select * from orders",
	    },
	    "index" : {
	        "index" : "jdbc",
	        "type" : "jdbc",
	        "bulk_size" : 100,
	        "max_bulk_requests" : 30,
	        "bulk_timeout" : "60s"
	    }
	}'


Stopping/deleting the river
---------------------------

	curl -XDELETE 'localhost:9200/_river/my_jdbc_river/'

