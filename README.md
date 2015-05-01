![JDBC](https://github.com/jprante/elasticsearch-river-jdbc/raw/master/src/site/resources/database-128.png)

Image by [icons8](http://www.iconsdb.com/icons8/?icon=database) Creative Commons Attribution-NoDerivs 3.0 Unported.

[Follow me on twitter](https://twitter.com/xbib)

# JDBC plugin for Elasticsearch
![Travis](https://travis-ci.org/jprante/elasticsearch-river-jdbc.png)

The Java Database Connection (JDBC) plugin allows to fetch data from JDBC sources for
indexing into [Elasticsearch](http://www.elasticsearch.org).

It is implemented as an [Elasticsearch plugin](http://www.elasticsearch.org/guide/reference/modules/plugins.html).

The JDBC plugin was designed for tabular data. If you have tables with many joins, the JDBC plugin
is limited in the way to reconstruct deeply nested objects to JSON and process object semantics like object identity.
Though it would be possible to extend the JDBC plugin with a maaping feature where all the object properties
could be specified, the current solution is focused on rather simple tabular data streams.

# This plugin is future-proof

Note, JDBC plugin is not only a river, but also a standalone module. Because Elasicsearch river API is deprecated,
this is an important feature. It means JDBC plugin is future-proof. All future versions of Elasticsearch
will be supported.

## Recent versions

| Release date | Plugin version | Elasticsearch version |
| -------------| ---------------| ----------------------|
| Apr 21, 2015 | 1.5.0.5        | 1.5.0                 |
| Apr 21, 2015 | 1.4.4.5        | 1.4.4                 |
| Mar 24, 2015 | 1.5.0.0        | 1.5.0                 |
| Mar 24, 2015 | 1.4.4.0        | 1.4.4                 |
| Feb 19, 2015 | 1.4.0.10       | 1.4.0                 |
| Jan 25, 2015 | 1.4.0.9        | 1.4.0                 |
| Jan  2, 2015 | 1.4.0.8        | 1.4.0                 |
| Dec 30, 2014 | 1.4.0.7        | 1.4.0                 |
| Dec 23, 2014 | 1.4.0.6        | 1.4.0                 |
| Dec 23, 2014 | 1.3.4.7        | 1.3.4                 |
| Dec 23, 2014 | 1.2.4.5        | 1.2.4                 |
| Dec 23, 2014 | 1.1.2.4        | 1.1.2                 |
| Dec 23, 2014 | 1.0.3.4        | 1.0.3                 |
| Dec 20, 2014 | 1.4.0.5        | 1.4.0                 |
| Dec 20, 2014 | 1.3.4.6        | 1.3.4                 |
| Dec 20, 2014 | 1.2.4.4        | 1.2.4                 |
| Dec 20, 2014 | 1.1.2.3        | 1.1.2                 |
| Dec 20, 2014 | 1.0.3.3        | 1.0.3                 |
| Oct 19, 2014 | 1.4.0.3.Beta1  | 1.4.0.Beta1           |
| Oct 19, 2014 | 1.3.4.4        | 1.3.4                 |
| Oct 19, 2014 | 1.2.4.2        | 1.2.4                 |
| Oct 19, 2014 | 1.1.2.1        | 1.1.2                 |
| Oct 19, 2014 | 1.0.3.1        | 1.0.3                 |

## Prerequisites

- a JDBC driver jar for your database. You should download a driver from the vendor site. Put the jar into JDBC plugin folder.

## Installation

    ./bin/plugin --install jdbc --url http://xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-river-jdbc/1.5.0.5/elasticsearch-river-jdbc-1.5.0.5-plugin.zip

Do not forget to restart the node after installing.

If you have installed Elasticsearch with other automation tools, like for example Homebrew,
you will need to locate your `ES_HOME` directory.  The easiest way to do this is by navigating to

    http://localhost:9200/_nodes?settings=true&pretty=true

Change into the home directory to invoke the `./bin/plugin` command line tool.


## Project docs

The Maven project site is available at [Github](http://jprante.github.io/elasticsearch-river-jdbc)

## Issues

All feedback is welcome! If you find issues, please post them at
[Github](https://github.com/jprante/elasticsearch-river-jdbc/issues)

# Documentation

## Two flavors: river or feeder

The plugin can operate as a river in "pull mode" or as a feeder in "push mode".
In feeder mode, the plugin runs in a separate JVM and can connect to a remote Elasticsearch cluster.

![Architecture](https://github.com/jprante/elasticsearch-river-jdbc/raw/master/src/site/resources/jdbc-river-feeder-architecture.png)

The relational data is internally transformed into structured JSON objects for the schema-less
indexing model of Elasticsearch documents.

![Simple data stream](https://github.com/jprante/elasticsearch-river-jdbc/raw/master/src/site/resources/simple-tabular-json-data.png)

Both ends are scalable. The plugin can fetch data from different RDBMS source in parallel, and multithreaded
bulk mode ensures high throughput when indexing to Elasticsearch.

![Scalable data stream](https://github.com/jprante/elasticsearch-river-jdbc/raw/master/src/site/resources/tabular-json-data.png)

## River or feeder?

The plugin comes in two flavors, river or feeder. Here are the differences.
Depending on your requirements, it is up to you to make a reasonable choice.

Note, the JDBC river code wraps the feeder code, there is no reinvention of anything.
Main difference is the different handling by starting/stopping the process by
a separate JVM in the feeder flavor.

| River                            | Feeder                             |
| ---------------------------------| -----------------------------------|
| standard method of Elasticsearch to connect to external sources and pull data | method to connect to external sources for pushing data into Elasticsearch |
| multiple river instances, many river types | no feeder types, feeder instances are separate JVMs |
| based on an internal index `_river` to keep state | based on a feeder document in the Elasticsearch index for maintaining state |
| does not scale, single local node only | scalable, not limited to single node, can connect to local or remote clusters |
| automatic failover and restart after cluster recovery | no failover, no restart |
| hard to supervise single or multi runs and interruptions | command line control of feeds, error exit code 1, crontab control |
| no standard method of  viewing river activity from within Elasticsearch | feed activity can be monitored by examining separate JVM |
| about to be deprecated by Elasticsearch core team | Feeder API provided by xbib, using advanced features supported by xbib libraries only. Part of upcoming "gatherer" API to support coordinated data harvesting by multiple ES nodes |

## Step-by-step guide to get a river running

Prerequisites:

A running MySQL database `test`, a table `orders`, and a user without name and password (default user)

A terminal / console with commands `curl` and `unzip` 

Internet access (of course)

1. Download elasticsearch (latest version that is compatible with JDBC plugin)

	`curl -OL https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.2.zip`

2. Unpack zip file into you favorite elasticsearch directory, we call it $ES_HOME

	`cd $ES_HOME`

	`unzip path/to/elasticsearch-1.4.2.zip`

3. Install JDBC plugin

	`./bin/plugin --install jdbc --url http://xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-river-jdbc/1.4.0.10/elasticsearch-river-jdbc-1.4.0.10.zip`

4. Download MySQL JDBC driver

	`curl -o mysql-connector-java-5.1.33.zip -L 'http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.33.zip/from/http://cdn.mysql.com/'`

5. Add MySQL JDBC driver jar to JDBC river plugin directory and set access permission for .jar file (at least chmod 644)

	`unzip mysql-connector-java-5.1.33.zip`

	`cp mysql-connector-java-5.1.33-bin.jar $ES_HOME/plugins/jdbc/`

	`chmod 644 $ES_HOME/plugins/jdbc/*`

6. Start elasticsearch from terminal window

	`./bin/elasticsearch`

7. Now you should notice from the log lines that a jdbc plugin has been installed (together with a support plugin)

8. Start another terminal, and create a JDBC river instance with name `my_jdbc_river`

    ```
	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select * from orders"
	    }
	}'
	```

9. The river runs immediately. It will run exactly once. Watch the log on the elasticsearch terminal
   for the river activity, some metric are written each minute. When the river fetched the data, 
   you can query for the data you just indexed with the following command

	`curl 'localhost:9200/jdbc/_search'`

10. Enjoy the result!

11. If you want to stop the `my_jdbc_river` river fetching data from the `orders` table after the
    quick demonstration, use this command

	`curl -XDELETE 'localhost:9200/_river/my_jdbc_river/'`

Now, if you want more fine-tuning, add a schedule for fetching data regularly,
you can change the index name, add more SQL statements, tune bulk indexing,
change the mapping, change the river creation settings.

## JDBC plugin parameters

The general schema of a JDBC river instance declaration is

	curl -XPUT 'localhost:9200/_river/<rivername>/_meta' -d '{
	    <river parameters>
	    "type" : "jdbc",
	    "jdbc" : {
	         <river definition>
	    }
	}'
	
Example:
	
	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : "select * from orders",
            "index" : "myindex",
            "type" : "mytype",
            ...	         
	    }
	}'

Multiple river sources are possible if an array is passed to the `jdbc` field.
These rivers are executed sequentially.

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	     <river parameters>
	    "type" : "jdbc",
	    "jdbc" : [ {
	         <river definition 1>
	    }, {
	         <river definition 2>
	    } ]
	}'

Concurrency of multi river sources can be controlled by the `concurrency` parameter:

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	     <river parameters>
	    "concurrency" : 2,
	    "type" : "jdbc",
	    "jdbc" : [ {
	         <river definition 1>
	    }, {
	         <river definition 2>
	    } ]
	}'


### Parameters outside of the `jdbc` block

`strategy` - the strategy of the JDBC plugin, currently implemented: `"simple"`, `"column"`

`schedule` - a single or a list of cron expressions for scheduled execution. Syntax is equivalent to the
Quartz cron expression format (see below).

`threadpoolsize` - a thread pool size for the scheduled executions for `schedule` parameter. If set to `1`, all jobs will be executed serially. Default is `4`.

`interval` - a time value for the delay between two river runs (default: not set)

`max_bulk_actions` - the length of each bulk index request submitted (default: 10000)

`max_concurrent_bulk_requests` - the maximum number of concurrent bulk requests (default: 2 * number of CPU cores)

`max_bulk_volume` - a byte size parameter for the maximum volume allowed for a bulk request (default: "10m")

`max_request_wait` - a time value for the maximum wait time for a response of a bulk request (default: "60s")

`flush_interval` - a time value for the interval period of flushing index docs to a bulk action (default: "5s")

### Parameters inside of the `jdbc` block

`url` - the JDBC driver URL

`user` - the JDBC database user

`password` - the JDBC database password

`sql`  - SQL statement(s), either a string or a list. If a statement ends with .sql, the statement is looked up in the file system. Example for a list of SQL statements:

	"sql" : [
	    {
	        "statement" : "select ... from ... where a = ?, b = ?, c = ?",
	        "parameter" : [ "value for a", "value for b", "value for c" ]
	    },
	    {
	        "statement" : "insert into  ... where a = ?, b = ?, c = ?",
	        "parameter" : [ "value for a", "value for b", "value for c" ],
	        "write" : "true"
	    },
	    {
	        "statement" : ...
	    }
	]

`sql.statement` - the SQL statement

`sql.write` - boolean flag, if true, the SQL statement is interpreted as an insert/update statement that needs write access (default: false).

`sql.callable` - boolean flag, if true, the SQL statement is interpreted as a JDBC `CallableStatement` for stored procedures (default: false).

`sql.parameter` - bind parameters for the SQL statement (in order). Some special values can be used with the following meanings:

  * `$now` - the current timestamp
  * `$job` - a job counter
  * `$count` - last number of rows merged
  * `$river.name` - the river name
  * `$last.sql.start` - a timestamp value for the time when the last SQL statement started
  * `$last.sql.end` - a timestamp value for the time when the last SQL statement ended
  * `$last.sql.sequence.start` - a timestamp value for the time when the last SQL sequence started
  * `$last.sql.sequence.end` - a timestamp value for the time when the last SQL sequence ended
  * `$river.state.started` - the timestamp of river start (from river state)
  * `$river.state.timestamp` - last timestamp of river activity (from river state)
  * `$river.state.counter` - counter from river state, counts the numbers of runs

`locale` - the default locale (used for parsing numerical values, floating point character. Recommended values is "en_US")

`timezone` - the timezone for JDBC setTimestamp() calls when binding parameters with timestamp values

`rounding` -  rounding mode for parsing numeric values. Possible values  "ceiling", "down", "floor", "halfdown", "halfeven", "halfup", "unnecessary", "up"

`scale` -  the precision of parsing numeric values

`autocommit` - `true` if each statement should be automatically executed. Default is `false`

`fetchsize` - the fetchsize for large result sets, most drivers use this to control the amount of rows in the buffer while iterating through the result set

`max_rows` - limit the number of rows fetches by a statement, the rest of the rows is ignored

`max_retries` - the number of retries to (re)connect to a database

`max_retries_wait` - a time value for the time that should be waited between retries. Default is "30s"

`resultset_type` - the JDBC result set type, can be TYPE_FORWARD_ONLY, TYPE_SCROLL_SENSITIVE, TYPE_SCROLL_INSENSITIVE. Default is TYPE_FORWARD_ONLY

`resultset_concurrency` - the JDBC result set concurrency, can be CONCUR_READ_ONLY, CONCUR_UPDATABLE. Default is CONCUR_UPDATABLE

`ignore_null_values` - if NULL values should be ignored when constructing JSON documents. Default is `false`

`prepare_database_metadata` - if the driver metadata should be prepared as parameters for acccess by the river.  Default is `false`

`prepare_resultset_metadata` - if the result set metadata should be prepared as parameters for acccess by the river.  Default is `false`

`column_name_map` - a map of aliases that should be used as a replacement for column names of the database. Useful for Oracle 30 char column name limit. Default is `null`

`query_timeout` - a second value for how long an SQL statement is allowed to be executed before it is considered as lost. Default is `1800`

`connection_properties` - a map for the connection properties for driver connection creation. Default is `null`


`index` - the Elasticsearch index used for indexing

`type` - the Elasticsearch type of the index used for indexing

`index_settings` - optional settings for the Elasticsearch index

`type_mapping` - optional mapping for the Elasticsearch index type

## Overview about the default parameter settings

	{
        "strategy" : "simple",
        "schedule" : null,
        "interval" : 0L,
        "threadpoolsize" : 4,
        "max_bulk_actions" : 10000,
        "max_concurrent_bulk_requests" : 2 * available CPU cores,
        "max_bulk_volume" : "10m",
        "max_request_wait" : "60s",
        "flush_interval" : "5s",
	    "jdbc" : {
	        "url" : null,
	        "user" : null,
	        "password" : null,
	        "sql" : null,
	        "locale" : Locale.getDefault().toLanguageTag(),
	        "timezone" : TimeZone.getDefault(),
	        "rounding" : null,
	        "scale" : 2,
	        "autocommit" : false,
	        "fetchsize" : 10, /* MySQL: Integer.MIN */
	        "max_rows" : 0,
	        "max_retries" : 3,
	        "max_retries_wait" : "30s",
	        "resultset_type" : "TYPE_FORWARD_ONLY",
	        "resultset_concurreny" : "CONCUR_UPDATABLE",
	        "ignore_null_values" : false,
	        "prepare_database_metadata" : false,
	        "prepare_resultset_metadata" : false,
	        "column_name_map" : null,
	        "query_timeout" : 1800,
	        "connection_properties" : null,
	        "index" : "jdbc",
	        "type" : "jdbc",
	        "index_settings" : null,
	        "type_mapping" : null,
	    }
	}

## Time scheduled execution of JDBC river

Setting a cron expression in the paramter `schedule` enables repeated (or time scheduled) runs of JDBC river.

You can also define a list of cron expressions (in a JSON array) to schedule for many
different time schedules.

Example of a `schedule` paramter:

        "schedule" : "0 0-59 0-23 ? * *"

This executes JDBC river every minute, every hour, all the days in the week/month/year.

The following documentation about the syntax of the cron expression is copied from the Quartz 
scheduler javadoc page.

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

## How to run a standalone JDBC feeder

A feeder can be started from a shell script. For this , the Elasticsearch home directory must be set in
the environment variable ES_HOME. The JDBC plugin jar must be placed in the same directory of the script,
together with JDBC river jar(s). 

Here is an example of a feeder bash script:

    #!/bin/sh

    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    
    # ES_HOME required to detect elasticsearch jars
    export ES_HOME=~es/elasticsearch-1.4.0.Beta1
    
    echo '
    {
        "elasticsearch" : {
             "cluster" : "elasticsearch",
             "host" : "localhost",
             "port" : 9300
        },
        "type" : "jdbc",
        "jdbc" : {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" :  "select *, page_id as _id from page",
            "treat_binary_as_string" : true,
            "index" : "metawiki"
          }
    }
    ' | java \
        -cp "${DIR}/*" \
        org.xbib.elasticsearch.plugin.jdbc.feeder.Runner \
        org.xbib.elasticsearch.plugin.jdbc.feeder.JDBCFeeder

How does it work?

- first the shell script finds out about the directory where the script is placed, and it is placed into a variable `DIR`

- second, the location of the Elasticsearch home is exported in a shell variable `ES_HOME`

- the classpath must be set to `DIR/*` to detect the JDBC plugin jar in the same directory of the script

- the "Runner" class is able to expand the classpath over the Elasticsearch jars in `ES_HOME/lib` and looks also in `ES_HOME/plugins/jdbc`

- the "Runner" class invokes the "JDBCFeeder", which reads a JSON file from stdin, which corresponds to a JDBC river definition

- the `elasticsearch` structure specifies the cluster, host, and port of a connection to an Elasticsearch cluster

The `jdbc` parameter structure in the definition is exactly the same as in a river.

It is possible to write an equivalent of this bash script for Windows. 
If you can send one to me for documentation on this page, I'd be very grateful.

## Structured objects

One of the advantage of SQL queries is the join operation. From many tables, new tuples can be formed.

	curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
	    "type" : "jdbc",
	    "jdbc" : {
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
	| relations | Large | Large            | Müller           |
	| relations | Large | Large            | Meier            |
	| relations | Large | Large            | Schulze          |
	| relations | Huge  | Huge             | Müller           |
	| relations | Huge  | Huge             | Meier            |
	| relations | Huge  | Huge             | Schulze          |
	| relations | Good  | Good             | Müller           |
	| relations | Good  | Good             | Meier            |
	| relations | Good  | Good             | Schulze          |
	| relations | Bad   | Bad              | Jones            |
	+-----------+-------+------------------+------------------+
	11 rows in set (0.00 sec)

will generate fewer JSON objects for the index `relations`.

	index=relations id=Big {"contact":{"employee":"Smith","customer":"Big"}}
	index=relations id=Large {"contact":{"employee":["Müller","Meier","Schulze"],"customer":"Large"}}
	index=relations id=Huge {"contact":{"employee":["Müller","Meier","Schulze"],"customer":"Huge"}}
	index=relations id=Good {"contact":{"employee":["Müller","Meier","Schulze"],"customer":"Good"}}
	index=relations id=Bad {"contact":{"employee":"Jones","customer":"Bad"}}

Note how the `employee` column is collapsed into a JSON array. The repeated occurence of the `_id` column
controls how values are folded into arrays for making use of the Elasticsearch JSON data model.


## Column names for JSON document construction

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
	_parent    the parent of this object
	_ttl       the time-to-live of this object
	_routing   the routing of this object

See also

http://www.elasticsearch.org/guide/reference/mapping/parent-field.html

http://www.elasticsearch.org/guide/reference/mapping/ttl-field.html

http://www.elasticsearch.org/guide/reference/mapping/routing-field.html


## Bracket notation for JSON array construction

When construction JSON documents, it is often the case you want to group SQL columns into a JSON object and
line them up into JSON arrays. For allowing this, a bracket notation is used to identify children
elements that repeat in each child.

Note, because of limitations in identifying SQL column groups, nested document structures may lead to
repititions of the same group. Fortunately, this is harmless to Elasticsearch queries.

Example:

| _id  | blog.name | blog.published      | blog.association[id] | blog.association[name] | blog.attachment[id]   | blog.attachment[name]    |
| ---- | ----------| --------------------| -------------------- | ---------------------- | --------------------- | ------------------------ |
| 4679 | Joe       | 2014-01-06 00:00:00 | 3917                 | John                   | 9450                  | /web/q/g/h/57436356.jpg  |
| 4679 | Joe       | 2014-01-06 00:00:00 | 3917                 | John                   | 9965                  | /web/i/s/q/GS3193626.jpg |
| 4679 | Joe       | 2014-01-06 00:00:00 | 3917                 | John                   | 9451                  | /web/i/s/q/GS3193626.jpg |

Result:

    {
        "blog" : {
            "attachment": [
                {
                    "name" : "/web/q/g/h/57436356.jpg",
                    "id" : "9450"
                },
                {
                    "name" : "/web/i/s/q/GS3193626.jpg",
                    "id" : "9965"
                },
                {
                    "name" : "/web/i/s/q/GS3193626.jpg",
                    "id" : "9451"
                }
            ],
            "name" : "Joe",
            "association" : [
                {
                    "name" : "John",
                    "id" : "3917"
                },
                {
                    "name" : "John",
                    "id" : "3917"
                },
                {
                    "name" : "John",
                    "id" : "3917"
                }
             ],
             "published":"2014-01-06 00:00:00"
         }
    }

## How to fetch a table?

For fetching a table, a simple "select *" (star) query can be used.
Star queries are the simplest variant of selecting data from a database.
They dump tables into Elasticsearch row-by-row. If no `_id` column name is given, IDs will be automatically generated.

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

## How to update a table?

The JDBC plugin allows to write data into the database only for maintenance purpose. 
It does not allow to inverse the river, that is, it not impossible to fill database tables from Elasticsearch 
indices with this plugin. Think of the river as a one-way street.

Writing back data into the database makes sense for acknowledging fetched data. 

Example:

    {
        "type" : "jdbc",
        "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
            "sql" : [
                {
                    "statement" : "select * from \"products\""
                },
                {
                    "statement" : "delete from \"products\" where \"_job\" = ?",
                    "parameter" : [ "$job" ]
                }
            ],
            "index" : "my_jdbc_river_index",
            "type" : "my_jdbc_river_type"
        }
    }

In this example, the DB administrator has prepared product rows and attached a `_job` column to it
to enumerate the product updates incrementally. The assertion is that Elasticsearch should 
delete all products from the database after they are indexed successfully. The parameter `$job`
is a counter which counts from the river start. The river state is saved in the cluster state,
so the counter is persisted throughout the lifetime of the cluster.

## How to select incremental data from a table?

It is recommended to use timestamps in UTC for synchronization. This example fetches
all product rows which has added since the last river run, using a millisecond resolution
column `mytimestamp`:

    {
        "type" : "jdbc",
        "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
            "sql" : [
                {
                    "statement" : "select * from \"products\" where \"mytimestamp\" > ?",
                    "parameter" : [ "$river.state.last_active_begin" ]
                }
            ],
            "index" : "my_jdbc_river_index",
            "type" : "my_jdbc_river_type"
        }
    }


## Stored procedures or callable statements

Stored procedures can also be used for fetchng data, like this example fo MySQL illustrates. 
See also [Using Stored Procedures](http://docs.oracle.com/javase/tutorial/jdbc/basics/storedprocedures.html)
from where the example is taken.

    create procedure GET_SUPPLIER_OF_COFFEE(
        IN coffeeName varchar(32), 
        OUT supplierName varchar(40)) 
        begin 
            select SUPPLIERS.SUP_NAME into supplierName 
            from SUPPLIERS, COFFEES 
            where SUPPLIERS.SUP_ID = COFFEES.SUP_ID 
            and coffeeName = COFFEES.COF_NAME; 
            select supplierName; 
        end

Now it is possible to call the procedure from the JDBC plugin and index the result in Elasticsearch.

    {
        "jdbc" : {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : [
                {
                    "callable" : true,
                    "statement" : "{call GET_SUPPLIER_OF_COFFEE(?,?)}",
                    "parameter" : [
                         "Colombian"
                    ],
                    "register" : {
                         "mySupplierName" : { "pos" : 2, "type" : "varchar" }
                    }
                }
            ],
            "index" : "my_jdbc_river_index",
            "type" : "my_jdbc_river_type"
        }
    }

Note, the `parameter` lists the input parameters in the order they should be applied, like in an
ordinary statement. The `register` declares a list of output parameters in the particular order
the `pos` number indicates. It is required to declare the JDBC type in the `type` attribute.
`mySupplierName`, the key of the output parameter, is used as the Elasticsearch field name specification,
like the column name specification in an ordinary SQL statement, because column names are not available
in callable statement result sets.

If there is more than one result sets returned by a callable statement,
the JDBC plugin enters a loop and iterates through all result sets.

# Monitoring the JDBC plugin state

While a river/feed is running, you can monitor the activity by using the `_state` command.

The `_state` command can show the state of a specific river or of all rivers, 
when an asterisk `*` is used as the river name.

The river state mechanism is specific to JDBC plugin implementation. It is part of the cluster metadata.

In the response, the field `started` will represent the time when the river/feeder was created.
The field `last_active_begin` will represent the last time when a river/feeder run had begun, and
the field `last_active_end` is null if th river/feeder runs, or will represent the last time the river/feeder 
has completed a run.

The `map` carries some flags for the river: `aborted`, `suspended`, and a `counter` for the number of
invocations on this node.

Example:

    curl 'localhost:9200/_river/jdbc/*/_state?pretty'
    {
      "state" : [ {
        "name" : "feeder",
        "type" : "jdbc",
        "started" : "2014-10-18T13:38:14.436Z",
        "last_active_begin" : "2014-10-18T17:46:47.548Z",
        "last_active_end" : "2014-10-18T13:42:57.678Z",
        "map" : {
          "aborted" : false,
          "suspended" : false,
          "counter" : 6
        }
      } ]
    }    

## Suspend

A running river can be suspended with 

    curl 'localhost:9200/_river/jdbc/my_jdbc_river/_suspend'

## Resume

A suspended river can be resumed with

    curl 'localhost:9200/_river/jdbc/my_jdbc_river/_resume'

# Advanced topics

## RiverSource, RiverMouth, RiverFlow

The JDBC river consists of three conceptual interfaces than can be implemented separately.

When you use the ``strategy`` parameter, the JDBC river tries to load additional classes before
falling back to the ``simple`` strategy.

You can implement your own strategy by adding your implementation jars to the plugin folder and
exporting the implementing classes in the ``META-INF/services`` directory. The ``RiverService`` looks up implementations for your favorite ``strategy`` before the JDBC river initializes.

So, it is easy to reuse or replace existing code, or adapt your own JDBC retrieval strategy
to the core JDBC river.

### RiverSource

The river source models the data producing side. Beside defining the JDBC connect parameters, it manages a dual-channel connection to the data producer for reading and for writing.
The reading channel is used for fetching data, while the writing channel can update the source.

The RiverSource API can be inspected at http://jprante.github.io/elasticsearch-river-jdbc/apidocs/org/xbib/elasticsearch/river/jdbc/RiverSource.html

### RiverMouth

The ``RiverMouth`` is the abstraction of the destination where all the data is flowing from the river source. It controls the resource usage of the bulk indexing method of Elasticsearch. Throttling is possible by limiting the number of bulk actions per request or by the maximum number of concurrent request.

The RiverMouth API can be inspected at http://jprante.github.io/elasticsearch-river-jdbc/apidocs/org/xbib/elasticsearch/river/jdbc/RiverSource.html

### RiverFlow

The ``RiverFlow`` is the abstraction to the thread which performs data fetching from the river source and transports it to the river mouth. A 'move' is considered a single step in the river live cycle. A river flow can be aborted.

The RiverFlow API can be inspected at http://jprante.github.io/elasticsearch-river-jdbc/apidocs/org/xbib/elasticsearch/river/jdbc/RiverFlow.html

## Strategies

The JDBC plugin can be configured for different methods of data transport.
Such methods of data transports are called a 'strategy'.

By default, the JDBC plugin implements a ``simple`` strategy.

## Simple strategy

This strategy contains the following steps of processing:

1. fetch data from the JDBC connection
2. build structured objects and move them to Elasticsearch for indexing or deleting

In the ``sql`` parameter of the river, a series of SQL statements can be defined which are executed at each river cycle to fetch the data.

## Your custom strategy

If you want to extend the JDBC plugin, for example by your custom password authentication, you could
extend the SimpleRiverSource. Then, declare your strategy classes in `META-INF/services`. Add your
jar to the classpath and add the `strategy` parameter to the river/feeder specifications.

# Examples

## PostgreSQL

1. Install PostgreSQL

   Example: PostgreSQL .dmg (Version 9.1.5) for Mac OS X from http://www.enterprisedb.com/products-services-training/pgdownload

   Filename: postgresql-9.1.5-1-osx.dmg

2. Install Elasticsearch

   Follow instructions on http://elasticsearch.org

3. Install JDBC plugin

   Check for the JDBC version under http://github.com/jprante/elasticsearch-river/jdbc

	    cd $ES_HOME
	   ./bin/plugin --install jdbc --url http://xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-river-jdbc/1.3.0.4/elasticsearch-river-jdbc-1.3.0.4-plugin.zip

4. Download PostgreSQL JDBC driver

   Check http://jdbc.postgresql.org/download.html

   Current version is JDBC4 Postgresql Driver, Version 9.1-902

   Filname postgresql-9.1-902.jdbc4.jar

5. Copy driver into river folder

   The reason is to include the JDBC driver into the Java classpath.

	    cp postgresql-9.1-902.jdbc4.jar $ES_HOME/plugins/river-jdbc/

6. Start Elasticsearch

   Just in the foreground to follow log messages on the console.

	    cd $ES_HOME
	    ./bin/elasticsearch

   Check if the river is installed correctly, Elasticsearch announces it in the second line logged. It must show ``loaded [jdbc-...]``.

	[2014-01-22 23:00:06,821][INFO ][node                     ] [Julie Power] version[...], pid[26152], build[c6155c5/2014-01-15T17:02:32Z]
	[2014-01-22 23:00:06,841][INFO ][node                     ] [Julie Power] initializing ...
	[2014-01-22 23:00:06,932][INFO ][plugins                  ] [Julie Power] loaded [jdbc-..., support-...], sites []

7. Create JDBC river

   This is just a basic example to a database `test` with user `fred` and password `secret`.
   The easiest method is using ``curl`` for a river creation via the REST interface.
   Use the port configured during PostgreSQL installation. The default is `5432`.
   ```
   curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
        "type" : "jdbc",
        "jdbc" : {
            "url" : "jdbc:postgresql://localhost:5432/test",
            "user" : "fred",
            "password" : "secret",
            "sql" : "select * from orders",
            "index" : "myindex",
            "type" : "mytype"
        }
   }'
   ```

8. Check log messages

   In case the user does not exist, Elasticsearch will log a message.

9. Repeat River creation until the river runs fine.


## MS SQL Server

1. Download Elasticsearch

2. Download SQL Server JDBC driver from [the vendor](http://msdn.microsoft.com/en-us/sqlserver/aa937724.aspx)

3. Put the `SQLJDBC4.jar` file in the lib folder of elasticsearch.

4. Download elasticsearch-river-jdbc using the plugin downloader. Navigate to the elasticsearch folder on your computer and run...
   ```
    ./bin/plugin --install jdbc --url ...
   ```

5. Set up the database you want to be indexed.
   [This includes allowing TCP/IP connections](http://stackoverflow.com/questions/2388042/connect-to-sql-server-2008-with-tcp-ip)

6. Start Elasticsearch
    ```
    ./elasticsearch.bat
    ```

7. Install a river like this
    ```
    curl -XPUT 'localhost:9200/_river/scorecards_river/_meta' -d '
    {
        "type" : "jdbc",
        "jdbc": {
            "url":"jdbc:sqlserver://localhost:1433;databaseName=ICFV",
            "user":"elasticsearch",
            "password":"elasticsearch",
            "sql":"select * from ScoreCards",
            "index" : "myindex",
            "type" : "mytype"
        }
    }
    ```

8. You should see the river parsing the incoming data from the database in the logfile.

## Index geo coordinates from MySQL in Elasticsearch

This minimalistic example can also be found at `bin/river/mysql/geo.sh`

- install MySQL e.g. in /usr/local/mysql

- start MySQL on localhost:3306 (default)

- prepare a 'test' database in MySQL

- create empty user '' with empty password '' (this user should exist as default user, otherwise set up a password and adapt the example)

- execute SQL in "geo.dump" /usr/local/mysql/bin/mysql test < src/test/resources/geo.dump

- then run this script: bash bin/river/mysql/geo.sh
    ```
    curl -XDELETE 'localhost:9200/_river/my_geo_river/'
    curl -XGET 'localhost:9200/_river/_refresh'
    curl -XDELETE 'localhost:9200/myjdbc'
    curl -XPOST 'localhost:9200/_river/my_geo_river/_meta' -d '
    {
        "type" : "jdbc",
        "jdbc" : {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "locale" : "en_US",
            "sql" : [
                {
                    "statement" : "select \"myjdbc\" as _index, \"mytype\" as _type, name as _id, city, zip, address, lat as \"location.lat\", lon as \"location.lon\" from geo"
                }
            ],
            "index" : "myjdbc",
            "type" : "mytype",
            "index_settings" : {
                "index" : {
                    "number_of_shards" : 1
                }
            },
            "type_mapping": {
                "mytype" : {
                    "properties" : {
                        "location" : {
                            "type" : "geo_point"
                        }
                    }
                }
            }
        }
    }
    '
    echo "sleeping while river should run..."
    sleep 10
    curl -XDELETE 'localhost:9200/_river/my_geo_river/'
    curl -XGET 'localhost:9200/myjdbc/_refresh'
    curl -XPOST 'localhost:9200/myjdbc/_search?pretty' -d '
    {
      "query": {
         "filtered": {
           "query": {
              "match_all": {
               }
           },
           "filter": {
               "geo_distance" : {
                   "distance" : "20km",
                   "location" : {
                        "lat" : 51.0,
                        "lon" : 7.0
                    }
                }
            }
         }
       }
    }'
    ```

## Oracle column name 30 character limit

Oracle imposes a 30 character limit on column name aliases. This makes it sometimes hard to define columns names for
Elasticsearch field names. For this, a column name map can be used like this:

    curl -XPUT 'localhost:9200/_river/my_oracle_river/_meta' -d '{
        "type" : "jdbc",
        "jdbc" : {
            "url" : "jdbc:oracle:thin:@//localhost/sid",
            "user" : "user",
            "password" : "password",
            "sql" : "select or_id as \"_id\", or_tan as \"o.t\", or_status as \"o.s\", stages.* from orders, stages where or_id = st_or_id and or_seqno = st_seqno",
            "column_name_map" : {
               "o" : "order",
               "t" : "transaction_id",
               "s" : "status"
            }
        }
    }'

## Connection properties for JDBC driver

For some JDBC drivers, advanced parameters can be passed that are not specified in the driver URL, 
but in the JDBC connection properties. You can specifiy connection properties like this:

    curl -XPUT 'localhost:9200/_river/my_oracle_river/_meta' -d '{
        "type" : "jdbc",
        "jdbc" : {
            "url" : "jdbc:oracle:thin:@//localhost:1521/sid",
            "user" : "user",
            "password" : "password",
            "sql" : "select ... from ...",
            "connection_properties" : {
                "oracle.jdbc.TcpNoDelay" : false,
                "useFetchSizeWithLongColumn" : false,
                "oracle.net.CONNECT_TIMEOUT" : 10000,
                "oracle.jdbc.ReadTimeout" : 50000
            }
        }
    }'


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
