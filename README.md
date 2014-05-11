![JDBC](https://github.com/jprante/elasticsearch-river-jdbc/raw/master/src/site/resources/database-128.png)

Image by [icons8](http://www.iconsdb.com/icons8/?icon=database) Creative Commons Attribution-NoDerivs 3.0 Unported.

# JDBC plugin for Elasticsearch
![Travis](https://travis-ci.org/jprante/elasticsearch-river-jdbc.png)

The Java Database Connection (JDBC) plugin allows to fetch data from JDBC sources for
indexing into [Elasticsearch](http://www.elasticsearch.org).

It is implemented as an [Elasticsearch plugin](http://www.elasticsearch.org/guide/reference/modules/plugins.html).

Creating a JDBC river is easy. Install the plugin. Download a JDBC driver jar from your vendor's site
(for example MySQL) and put the jar into the folder of the plugin `$ES_HOME/plugins/river-jdbc`.
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

## Plugin works as a river or a feeder

The plugin can operate as a river in "pull mode" or as a feeder in "push mode".
In feeder mode, the plugin runs in a separate JVM and can connect to a remote Elasticsearch cluster.

![Architecture](https://github.com/jprante/elasticsearch-river-jdbc/raw/master/src/site/resources/jdbc-river-feeder-architecture.png)

The relational data is internally transformed into structured JSON objects for the schema-less
indexing model of Elasticsearch documents.

![Simple data stream](https://github.com/jprante/elasticsearch-river-jdbc/raw/master/src/site/resources/simple-tabular-json-data.png)

Both ends are scalable. The plugin can fetch data from different RDBMS source in parallel, and multithreaded
bulk mode ensures high throughput when indexing to Elasticsearch.

![Scalable data stream](https://github.com/jprante/elasticsearch-river-jdbc/raw/master/src/site/resources/tabular-json-data.png)

## Versions

| ES version    | Plugin     | Release date |
| ------------- | -----------| -------------|
| 1.1.0         | 1.1.0.1    | May 10, 2014 |

## Prerequisites

- a JDBC driver jar for your database. You should download a driver from the vendor site. Put the jar into JDBC plugin folder.

## Installation

    ./bin/plugin --install jdbc --url http://xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-river-jdbc/1.1.0.1/elasticsearch-river-jdbc-1.1.0.1-plugin.zip

Do not forget to restart the node after installing.

If you have installed Elasticsearch with other automation tools, like for example Homebrew,
you will need to locate your `ES_HOME` directory.  The easiest way to do this is by navigating to

    localhost:9200/_cluster/nodes?settings=true&pretty=true

Change into this directory to invoke the `./bin/plugin` command line tool.

## Checksum

| File                                         | SHA1                                     |
| ---------------------------------------------| -----------------------------------------|
| elasticsearch-river-jdbc-1.1.0.1-plugin.zip  | 1065a30897beddd4e37cb63ca40500a02319dbe7 |

## Project docs

The Maven project site is available at [Github](http://jprante.github.io/elasticsearch-river-jdbc)

# Documentation


## River or feeder?

The plugin comes in two flavors, river or feeder. Here are the differences.
Depending on your requirements, it is up to you to make a choice.

Note, the JDBC river code wraps the feeder code, there is no reinvention of anything.
Main difference is the different handling by starting/stopping a separate JVM.

| River                            | Feeder                             |
| ---------------------------------| -----------------------------------|
| standard method of Elasticsearch to connect to external sources and pull data
| method to connect to external sources for pushing data into Elasticsearch |
| multiple river instances, many river types
| no feeder types, feeder instances are separate JVMs |
| based on an internal index `_river` to keep state      
| based on a feeder document in the Elasticsearch index for maintaining state |
| does not scale, single local node only
| scalable, not limited to single node, can connect to local or remote clusters |
| automatic failover and restart after cluster recovery
| no failover, no restart |
| hard to supervise single or multi runs and interruptions
| command line control of feeds, error exit code 1, crontab control |
| no standard method of  viewing river activity from within Elasticsearch
| feed activity can be monitored by examining separate JVM |
| about to be deprecated by Elasticsearch core team
| Feeder API provided by xbib, using advanced features supported by xbib libraries only.
Part of upcoming "gatherer" API to support coordinated data harvesting by multiple ES nodes |

## River start

Prerequisites:

A running MySQL database `test`, a table `orders`, and a user without name and password (default user)

A terminal / console with commands `curl` and `unzip` and Internet access (of course)

1. Download elasticsearch

	`curl -OL https://github.com/downloads/elasticsearch/elasticsearch/elasticsearch-1.1.0.0.zip`

2. Unpack zip file into you favorite elasticsearch directory, we call it $ES_HOME

	`cd $ES_HOME`

	`unzip path/to/elasticsearch-1.1.0.0.zip`

3. Install JDBC plugin

	`./bin/plugin --install jdbc --url http://xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-river-jdbc/1.1.0.1/elasticsearch-river-jdbc-1.1.0.1-plugin.zip`

4. Download MySQL JDBC driver

	`curl -o mysql-connector-java-5.1.28.zip -L 'http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.28.zip/from/http://cdn.mysql.com/'`

5. Add MySQL JDBC driver jar to JDBC river plugin directory and set access permission for .jar file (at least chmod 644)

	`cp mysql-connector-java-5.1.28-bin.jar $ES_HOME/plugins/jdbc/`
	`chmod 644 $ES_HOME/plugins/jdbc/`

6. Start elasticsearch from terminal window

	`./bin/elasticsearch`

7. Now you should notice from the log lines that a jdbc plugin has been installed (together with a support plugin)

8. Start another terminal, and create a JDBC river instance with name `my_jdbc_river`

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select * from orders"
	    }
	}'

9. The river runs immediately. It will run exactly once. Watch the log on the elasticsearch terminal
   for the river activity. When the river fetched the data, you can query your elasticsearch node
   for the data you just indexed with the following `curl` command

	`curl'localhost:9200/jdbc/_search?pretty&q=*'`

10. Enjoy the result!

11. If you want to stop the `my_jdbc_river` river fetching data from the `orders` table after the
    quick demonstration, use this command:

	`curl -XDELETE 'localhost:9200/_river/my_jdbc_river'``

Now, if you want more fine-tuning, add a schedule for fetching data regularly,
you can change the index name, add more SQL statements, tune bulk indexing,
change the mapping, change the river creation settings.

## Plugin parameters

`strategy` -the strategy of the JDBC plugin, currently implemented: "simple", "column"

`url` - the JDBC driver URL

`user` - the JDBC database user

`password` - the JDBC database password

`sql`  - SQL statement(s), either a string or a list. If a statement ends with .sql, the statement is looked up in the file system. Example for a list of SQL statements:

	"sql" : [
	    {
	        "statement" : "select ... from ... where a = ?, b = ?, c = ?",
	        "parameter" : [ "value for a", "value for b", "value for c" ],
	        "callable" : false
	    },
	    {
	        "statement" : ...
	    }
	]

`sql.statement` - the SQL statement

`sql.parameter` - bind parameters for the SQL statement (in order)

`sql.callable` - boolean flag, if true, the SQL statement is interpreted as a JDBC CallableStatement (default: false)

`locale` - the default locale (used for parsing numerical values, floating point character. Recommended values is "en_US")

`rounding` -  rounding mode for parsing numeric values. Possible values  "ceiling", "down", "floor", "halfdown", "halfeven", "halfup", "unnecessary", "up"

`scale` -  the precision of parsing numeric values

`autocommit` -  `true` if each statement should be automatically executed. Default is `false`

`fetchsize` - the fetchsize for large result sets, most drivers use this to control the amount of rows in the buffer while iterating through the result set

`max_rows` - limit the number of rows fetches by a statement, the rest of the rows is ignored

`max_retries` - the number of retries to (re)connect to a database

`max_retries_wait` - (timev alue) the time that should be waited between retries. Default is "30s"

`index` - the Elasticsearch index used for indexing

`type` - the Elasticsearch type of the index used for indexing

`index_settings` - optional settings for the Elasticsearch index

`type_mapping` - optional mapping for the Elasticsearch index type

`maxbulkactions` - the length of each bulk index request submitted

`maxconcurrrentbulkactions` - the maximum number of concurrent bulk requests

`schedule` - a single or a list of cron expressions for scheduled execution. Syntax is equivalent to the
Quartz cron expression format (see below).

## Default parameter settings

	{
	    "jdbc" :{
	        "strategy" : "simple",
	        "url" : null,
	        "user" : null,
	        "password" : null,
	        "sql" : null,
	        "schedule" : null,
	        "rounding" : null,
	        "scale" : 2,
	        "autocommit" : false,
	        "fetchsize" : 10, /* Integer.MIN for MySQL */
	        "max_rows" : 0,
	        "max_retries" : 3,
	        "max_retries_wait" : "30s",
	        "locale" : Locale.getDefault().toLanguageTag(),
	        "index" : "jdbc",
	        "type" : "jdbc",
	        "index_settings" : null,
	        "type_mapping" : null,
	        "maxbulkactions" : 1000,
	        "maxconcurrentbulkactions" : 4 * available CPU cores,
	    }
	}

## Obsolete parameters

In older versions, the following parameters were available. THey are no longer supported.

`driver` - Class name of JDBC river. Since JDBC plugin requires JDBC Version 4 (or higher), which is
included in Java 6, this parameter is not used any more.

`poll` - interval for waiting between river invocations. Replaced by `schedule`

`bulk_size` - renamed to `maxbulkactions`

`max_bulk_requests` - renamed to `maxconcurrrentbulkactions`

`bulk_flush_interval` - no longer supported, replaced by internal flush invocations

## Cron expression syntax

The following documentation is copied from the Quartz scheduler javadoc page.

Cron expressions provide the ability to specify complex time combinations such as
"At 8:00am every Monday through Friday" or "At 1:30am every last Friday of the month".

Cron expressions are comprised of 6 required fields and one optional field separated by
white space. The fields respectively are described as follows:

| Field Name      | Allowed Values      | Allowed Special Characters  |
| --------------- | ------------------- | ----------------------------|
| Seconds         | 0-59                | , - * / |
| Minutes         | 0-59                | , - * / |
| Hours           | 0-23                | , - * / |
| Day-of-month    | 1-31                | , - * ? / L W |
| Month           | 1-12 or JAN-DEC     | , - * / |
| Day-of-Week     | 1-7 or SUN-SAT      | , - * ? / L # |
| Year (Optional) | empty, 1970-2199    | , - * / |

The '*' character is used to specify all values. For example, "*" in the minute field means "every minute".

The '?' character is allowed for the day-of-month and day-of-week fields.
It is used to specify 'no specific value'.
This is useful when you need to specify something in one of the two fields, but not the other.

The '-' character is used to specify ranges For example "10-12" in the hour field means
"the hours 10, 11 and 12".

The ',' character is used to specify additional values. For example "MON,WED,FRI" in the
day-of-week field means "the days Monday, Wednesday, and Friday".

The '/' character is used to specify increments. For example "0/15" in the seconds field means
"the seconds 0, 15, 30, and 45". And "5/15" in the seconds field means "the seconds 5, 20, 35, and 50".
Specifying '*' before the '/' is equivalent to specifying 0 is the value to start with.
Essentially, for each field in the expression, there is a set of numbers that can be turned on or off.
For seconds and minutes, the numbers range from 0 to 59.
For hours 0 to 23, for days of the month 0 to 31, and for months 1 to 12.
The "/" character simply helps you turn on every "nth" value in the given set.
Thus "7/6" in the month field only turns on month "7", it does NOT mean every 6th month,
please note that subtlety.

The 'L' character is allowed for the day-of-month and day-of-week fields.
This character is short-hand for "last", but it has different meaning in each of the two fields.
For example, the value "L" in the day-of-month field means "the last day of the month" - day
31 for January, day 28 for February on non-leap years. If used in the day-of-week field by itself,
it simply means "7" or "SAT". But if used in the day-of-week field after another value,
it means "the last xxx day of the month" - for example "6L" means "the last friday of the month".
You can also specify an offset from the last day of the month, such as "L-3" which would mean
the third-to-last day of the calendar month. When using the 'L' option, it is important not
to specify lists, or ranges of values, as you'll get confusing/unexpected results.

The 'W' character is allowed for the day-of-month field. This character is used to specify
the weekday (Monday-Friday) nearest the given day. As an example, if you were to specify "15W"
as the value for the day-of-month field, the meaning is: "the nearest weekday to the 15th of the month".
So if the 15th is a Saturday, the trigger will fire on Friday the 14th.
If the 15th is a Sunday, the trigger will fire on Monday the 16th.
If the 15th is a Tuesday, then it will fire on Tuesday the 15th.
However if you specify "1W" as the value for day-of-month, and the 1st is a Saturday,
the trigger will fire on Monday the 3rd, as it will not 'jump' over the boundary of a month's days.
The 'W' character can only be specified when the day-of-month is a single day,
not a range or list of days.

The 'L' and 'W' characters can also be combined for the day-of-month expression to yield 'LW',
which translates to "last weekday of the month".

The '#' character is allowed for the day-of-week field. This character is used to specify
"the nth" XXX day of the month. For example, the value of "6#3" in the day-of-week field means
the third Friday of the month (day 6 = Friday and "#3" = the 3rd one in the month).
Other examples: "2#1" = the first Monday of the month and "4#5" = the fifth Wednesday of the month.
Note that if you specify "#5" and there is not 5 of the given day-of-week in the month,
then no firing will occur that month. If the '#' character is used, there can only be
one expression in the day-of-week field ("3#1,6#3" is not valid, since there are two expressions).

The legal characters and the names of months and days of the week are not case sensitive.

Note: Support for specifying both a day-of-week and a day-of-month value is not complete
(you'll need to use the '?' character in one of these fields).
Overflowing ranges is supported - that is, having a larger number on the left hand side than the right.
You might do 22-2 to catch 10 o'clock at night until 2 o'clock in the morning, or you might have NOV-FEB.
It is very important to note that overuse of overflowing ranges creates ranges that don't make sense
and no effort has been made to determine which interpretation CronExpression chooses.
An example would be "0 0 14-6 ? * FRI-MON".

## Feeder start

In the `bin` directory, you find some river/feeder examples.

A feed can be startet from the $ES_HOME/plugins/jdbc folder. Create a `bin` folder so it is easy to
maintain feeder script side by side with the river.

The feeder script must include the Elasticsearch core libraries into the classpath. Note the `-cp`
parameter.

Here is an example of a feed script in `$ES_HOME/plugins/jdbc/bin/feeder/oracle.create.sh`

    #!/bin/sh

    java="/usr/bin/java"
    #java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"
    #java="/usr/java/jdk1.8.0/bin/java"

    echo '
    {
        "concurrency" : 1,
        "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
        "client" : "bulk",
        "jdbc" : {
            "url" : "jdbc:oracle:thin:@//host:1521/sid",
            "user" : "user",
            "password" : "password",
            "sql" : "select or_id as \"_id\", or_tan as \"tan\" from orders",
            "index" : "myoracle",
            "type" : "myoracle",
            "index_settings" : {
                "index" : {
                    "number_of_shards" : 1,
                    "number_of_replica" : 0
                }
            }
        }
    }
    ' | ${java} \
        -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
        org.xbib.elasticsearch.plugin.feeder.Runner \
        org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder

The `jdbc` parameter structure is exactly the same as in a river.

The feeder is invoked by `org.xbib.elasticsearch.plugin.feeder.Runner org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder`
and understands some more parameters. In this example, the default parameters are shown.

`elasticsearch` - an URI pointing to a host of an Elasticsearch cluster

`client` - the value `bulk` enables a transport client with the vanilla BulkProcessor

`concurrency` - how many `jdbc` jobs should be executed in parallel

In the example, you can also see that you can change your favorite `java` executable when
executing a feed.

## Column names for drivin JSON document construction

In SQL, each column may be labeled. This label is used by the JDBC plugin for JSON document
construction. The dot is the path separator for the document strcuture.

For example, this JDBC river

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product"
	    }
	}'

the labeled columns are `product.name`, `product.customer.name`, and `product.customer.bill`.

A data example:

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

The structured objects constructed from these columns are

	id=0 {"product":{"name":"Apples","customer":{"bill":1.0,"name":"Big"}}}
	id=1 {"product":{"name":"Bananas","customer":{"bill":2.0,"name":"Large"}}}
	id=2 {"product":{"name":"Oranges","customer":{"bill":6.0,"name":"Huge"}}}
	id=3 {"product":{"name":"Apples","customer":{"bill":2.0,"name":"Good"}}}
	id=4 {"product":{"name":"Oranges","customer":{"bill":9.0,"name":"Bad"}}}

There are column labels with an underscore as prefix that are mapped to special Elasticsearch document parameters for indexing:

	_index     the index this object should be indexed into
	_type      the type this object should be indexed into
	_id        the id of this object
	_version   the version of this object
	_parent    the parent,
	_ttl       the time-to-live of this object
	_routing   the routing of this object

See also

http://www.elasticsearch.org/guide/reference/mapping/parent-field.html

http://www.elasticsearch.org/guide/reference/mapping/ttl-field.html

http://www.elasticsearch.org/guide/reference/mapping/routing-field.html


## JSON array construction




# Frequently asked questions

## How to fetch a table?

For fetching a table, a simple "select *" (star) query can be used.
Star queries are the simplest variant of selecting data from a database.
They dump tables into Elasticsearch row-by-row.

For example, this river

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select * from orders"
	    }
	}'

and this table

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

will result into the following JSON documents

	id=<random> {"product":"Apples","created":null,"department":"American Fruits","quantity":1,"customer":"Big"}
	id=<random> {"product":"Bananas","created":null,"department":"German Fruits","quantity":1,"customer":"Large"}
	id=<random> {"product":"Oranges","created":null,"department":"German Fruits","quantity":2,"customer":"Huge"}
	id=<random> {"product":"Apples","created":1338501600000,"department":"German Fruits","quantity":2,"customer":"Good"}
	id=<random> {"product":"Oranges","created":1338501600000,"department":"English Fruits","quantity":3,"customer":"Bad"}




# Advanced topics

## RiverSource, RiverMouth, RiverFlow

## Structured objects

## Strategies

## Support plugin

# Some real-world examples

## Setting up the river with PostgreSQL

## Setting up the river with MS SQL Server

## Index geo coordinates form MySQL in Elasticsearch

# License

Elasticsearch JDBC Plugin

Copyright (C) 2012-2014 Jörg Prante

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
