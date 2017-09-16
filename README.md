# JDBC importer for Elasticsearch

The Java Database Connection (JDBC) importer allows to fetch data from JDBC sources for
indexing into [Elasticsearch](http://www.elasticsearch.org).

The JDBC importer is hard to use. In this version, only care about transfer data to ES and add ability for tracking and recover data.
### TODO: separate ES config to json and sync sql to sql script file

Assuming you have a table of name `orders` with a primary key in column `id`, 
you can issue this from the command line

    bin=$JDBC_IMPORTER_HOME/bin
    lib=$JDBC_IMPORTER_HOME/lib
    echo '{
        "type" : "jdbc",
        "jdbc" : {
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : "select *, id as _id from orders"
        }
    }' | java \
           -cp "${lib}/*" \
           -Dlog4j.configurationFile=${bin}/log4j2.xml \
           org.xbib.tools.Runner \
           org.xbib.jdbc.JdbcPipeline

And that's it. Now you can check your Elasticsearch cluster for the index `jdbc` or your Elasticsearch logs
about what happened.



# Documentation

The relational data is internally transformed into structured JSON objects for the schema-less
indexing model of Elasticsearch documents.

The importer can fetch data from RDBMS while multithreaded bulk mode ensures high throughput when 
indexing to Elasticsearch.

## JDBC importer definition file

The general form of a JDBC import specification is a JSON object.

	{
	    "type" : "jdbc",
	    "jdbc" : {
	         <definition>
	    }
	}
	
Example:
	
	{
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
	}

The importer can either be executed via stdin (for example with echo)

    bin=$JDBC_IMPORTER_HOME/bin
    lib=$JDBC_IMPORTER_HOME/lib
    echo '{
      ...
    }' | java \
		-cp "${lib}/*" \
		-Dlog4j.configurationFile=${bin}/log4j2.xml \
		org.xbib.tools.Runner \
		org.xbib.jdbc.JdbcPipeline

or with explicit file name parameter from command line. Here is an example
where `statefile.json` is a file which is loaded before execution.

	java \
		-cp "${lib}/*" \
		-Dlog4j.configurationFile=${bin}/log4j2.xml \
		org.xbib.tools.Runner \
		org.xbib.jdbc.JdbcPipeline \
		statefile.json

This style is convenient for subsequent execution controlled by the `statefile` parameter
if `statefile` is set to `statefile.json`.

### Parameters

Here is the list of parameters for the `jdbc` block in the definition.


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

`sql.parameter` - bind parameters for the SQL statement (in order). Some special values can be used with the following meanings:

   * `$now` - the current timestamp
   * `$state` - the state, one of: BEFORE_FETCH, FETCH, AFTER_FETCH, IDLE, EXCEPTION
   * `$metrics.counter` - a counter
   * `$lastrowcount` - number of rows from last statement
   * `$lastexceptiondate` - SQL timestamp of last exception
   * `$lastexception` - full stack trace of last exception
   * `$metrics.lastexecutionstart` - SQL timestamp of the time when last execution started
   * `$metrics.lastexecutionend` - SQL timestamp of the time when last execution ended
   * `$metrics.totalrows` - total number of rows fetched
   * `$metrics.totalbytes` - total number of bytes fetched
   * `$metrics.failed` - total number of failed SQL executions
   * `$metrics.succeeded` - total number of succeeded SQL executions
   
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

`detect_json` - if json structures in SQL columns should be parsed when constructing JSON documents. Default is `true`

`prepare_database_metadata` - if the driver metadata should be prepared as parameters.  Default is `false`

`prepare_resultset_metadata` - if the result set metadata should be prepared as parameters.  Default is `false`

`column_name_map` - a map of aliases that should be used as a replacement for column names of the database. Useful for Oracle 30 char column name limit. Default is `null`

`query_timeout` - a second value for how long an SQL statement is allowed to be executed before it is considered as lost. Default is `1800`

`connection_properties` - a map for the connection properties for driver connection creation. Default is `null`

`schedule` - a single or a list of cron expressions for scheduled execution. Syntax is equivalent to the
Quartz cron expression format (see below for syntax)

`threadpoolsize` - a thread pool size for the scheduled executions for `schedule` parameter. If set to `1`, all jobs will be executed serially. Default is `4`.

`interval` - a time value for the delay between two runs (default: not set)

`elasticsearch.cluster` - Elasticsearch cluster name

`elasticsearch.host` - array of Elasticsearch host specifications (host name or `host:port`)

`elasticsearch.port` -  port of Elasticsearch host

`elasticsearch.autodiscover` - if `true`, JDBC importer will try to connect to all cluster nodes. Default is `false`

`max_bulk_actions` - the length of each bulk index request submitted (default: 10000)

`max_concurrent_bulk_requests` - the maximum number of concurrent bulk requests (default: 2 * number of CPU cores)

`max_bulk_volume` - a byte size parameter for the maximum volume allowed for a bulk request (default: "10m")

`max_request_wait` - a time value for the maximum wait time for a response of a bulk request (default: "60s")

`flush_interval` - a time value for the interval period of flushing index docs to a bulk action (default: "5s")

`index` - the Elasticsearch index used for indexing

`type` - the Elasticsearch type of the index used for indexing

`index_settings` - optional settings for the Elasticsearch index

`type_mapping` - optional mapping for the Elasticsearch index type

`statefile` - name of a file where the JDBC importer reads or writes state information 

`metrics.lastexecutionstart` - the UTC date/time of the begin of the last execution of a single fetch

`metrics.lastexecutionend` - the UTC date/time of the end of the last execution of a single fetch

`metrics.counter` - a counter for metrics, will be incremented after each single fetch

`metrics.enabled` - if `true`, metrics logging is enabled. Default is `false`

`metrics.interval` - the interval between metrics logging. Default is 30 seconds.

`metrics.logger.plain` - if `true`, write metrics log messages in plain text format. Default is `false`

`metrics.logger.json` - if `true`, write metric log messages in JSON format. Default is `false`

## Overview about the default parameter settings

	{
	    "jdbc" : {
	        "url" : null,
	        "user" : null,
	        "password" : null,
	        "sql" : null,
	        "locale" : /* equivalent to Locale.getDefault().toLanguageTag() */,
	        "timezone" : /* equivalent to TimeZone.getDefault() */,
	        "rounding" : null,
	        "scale" : 2,
	        "autocommit" : false,
	        "fetchsize" : 10, /* if URL contains MySQL JDBC driver URL, this is Integer.MIN */
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
			"schedule" : null,
			"interval" : 0L,
			"threadpoolsize" : 1,
	        "index" : "jdbc",
	        "type" : "jdbc",
	        "index_settings" : null,
	        "type_mapping" : null,
			"max_bulk_actions" : 10000,
			"max_concurrent_bulk_requests" : 2 * available CPU cores,
			"max_bulk_volume" : "10m",
			"max_request_wait" : "60s",
			"flush_interval" : "5s"
	    }
	}

## Time scheduled execution

Setting a cron expression in the parameter `schedule` enables repeated (or time scheduled) runs.

You can also define a list of cron expressions (in a JSON array) to schedule for many
different time schedules.

Example of a `schedule` parameter:

        "schedule" : "0 0-59 0-23 ? * *"

This executes JDBC importer every minute, every hour, all the days in the week/month/year.

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

## Structured objects

One of the advantage of SQL queries is the join operation. From many tables, new tuples can be formed.

	{
	    "type" : "jdbc",
	    "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select \"relations\" as \"_index\", orders.customer as \"_id\", orders.customer as \"contact.customer\", employees.name as \"contact.employee\" from orders left join employees on employees.department = orders.department order by _id"
	    }
	}

For example, these rows from SQL

	mysql> select "relations" as "_index", orders.customer as "_id", orders.customer as "contact.customer", employees.name as "contact.employee"  from orders left join employees on employees.department = orders.department order by _id;
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

Note how the `employee` column is collapsed into a JSON array. The repeated occurrence of the `_id` column
controls how values are folded into arrays for making use of the Elasticsearch JSON data model. Make sure your SQL query is ordered by `_id`.


## Column names for JSON document construction

In SQL, each column may be labeled. This label is used by the JDBC importer for JSON document
construction. The dot is the path separator for the document strcuture.

For example

	{
	    "type" : "jdbc",
	    "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product"
	    }
	}

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
repetitions of the same group. Fortunately, this is harmless to Elasticsearch queries.

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

For fetching a table, a "select \*" (star) query can be used.
Star queries are the simplest variant of selecting data from a database.
They dump tables into Elasticsearch row-by-row. If no `_id` column name is given, IDs will be automatically generated.

For example

	{
	    "type" : "jdbc",
	    "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
	        "user" : "",
	        "password" : "",
	        "sql" : "select * from orders"
	    }
	}

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

The JDBC importer allows to write data into the database for maintenance purpose. 

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
            "index" : "my_jdbc_index",
            "type" : "my_jdbc_type"
        }
    }

In this example, the DB administrator has prepared product rows and attached a `_job` column to it
to enumerate the product updates incrementally. The assertion is that Elasticsearch should 
delete all products from the database after they are indexed successfully. The parameter `$job`
is a counter. The importer state is saved in a file, so the counter is persisted.

## How to select incremental data from a table?

It is recommended to use timestamps in UTC for synchronization. This example fetches
all product rows which has added since the last run, using a millisecond resolution
column `mytimestamp`:

    {
        "type" : "jdbc",
        "jdbc" : {
	        "url" : "jdbc:mysql://localhost:3306/test",
            "statefile" : "statefile.json",
	        "user" : "",
	        "password" : "",
            "sql" : [
                {
                    "statement" : "select * from products where mytimestamp > ?",
                    "parameter" : [ "$metrics.lastexecutionstart" ]
                }
            ],
            "index" : "my_jdbc_index",
            "type" : "my_jdbc_type"
        }
    }

the first time you run the script, it will generate the statefile.json file like this
```
{
  "type" : "jdbc",
  "jdbc" : { 
    "password" : "",
    "index" : "my_jdbc_index",
    "statefile" : "statefile.json",
    "metrics" : { 
      "lastexecutionstart" : "2016-03-27T06:37:09.165Z",
      "lastexecutionend" : "2016-03-27T06:37:09.501Z",
      "counter" : "1" 
    },  
    "type" : "my_jdbc_type",
    "user" : "",
    "url" : "jdbc:mysql://localhost:3306/test",
    "sql" : [ { 
      "statement" : "select * from products where mytimestamp > ?", 
      "parameter" : [ "$metrics.lastexecutionstart" ]
    } ] 
  }
}
```
after this, you can select incremental data from table.

# How to import from a CSV file?

Importing from a CSV is easy because a CSV JDBC driver is included.

Try something like this

	{
		"type" : "jdbc",
		"jdbc" : {
			"driver" : "org.xbib.jdbc.csv.CsvDriver",
			"url" : "jdbc:xbib:csv:mydatadir?columnTypes=&separator=,",
			"user" : "",
			"password" : "",
			"sql" : "select * from mycsvfile"
		}
	}

where

`mydatadir` - path to the directory where the CSV file exists

`mycsvfile` - the name of the file

`columnTypes` - column types will be inferred. Default is `String`, where column types will be all set to  string

`separator` - the column separator

For a full list of the CSV JDBC driver options, see
https://github.com/jprante/jdbc-driver-csv

# Persisted state

The JDBC importer writes the state after each execution step into a state file which can be set by the
parameter `statefile`, see above in the parameter documentation. Default setting is not writing 
to state file.

Example:

    "sql" : ...,
    "statefile" : "statefile.json",
    ...

You can use the `statefile` as input for a next JDBC importer invocation, once it is saved. 
This is useful if you have to restart the JDBC importer. Because the statefile is written
in prettified JSON, it is also possible to adjust the 
settings in the statefile if you need to synchronize with the JDBC source.

Note: there must be enough space on disk to write the state file. If disk is full,
JDBC importer will write zero length files and give error messages in the importer log.

# Monitoring the JDBC importer

Metrics logging can be enabled to watch for the current transfer statistics. 

Example:

    "sql" : ...,
    "schedule" : ...,
    "statefile" : "statefile.json",
    "metrics" : {
        "enabled" : true,
        "interval" : "1m",
        "logger" : {
            "plain" : false,
            "json" : true
        }
    }

This configuration enables metrics logging, sets the metrics logging interval to one minute,
and switches form plain loggin to JSON logging.

In the `log4j2.xml` configuration file, you can set up how to log. The loggers used for metrics logging are

`metrics.source.plain` - for plain format logging of the source

`metrics.sink.plain` - for plain format logging of the sink

`metrics.source.json` - for JSON format logging of the source

`metrics.sink.json` - for JSON format logging of the sink

See also the parameter documentation above.

# Developer notes

## Source, Sink, Context

The JDBC importer consists of three conceptual interfaces than can be implemented separately.

### Source

The `Source` models the data producing side. Beside defining the JDBC connect parameters, 
it manages a dual-channel connection to the data producer for reading and for writing.
The reading channel is used for fetching data, while the writing channel can update the source.

The `Source` API can be inspected at 
http://jprante.github.io/elasticsearch-jdbc/apidocs/org/xbib/elasticsearch/jdbc/strategy/Source.html

### Sink

The `Sink` is the abstraction of the destination where all the data is flowing from the source. 
It controls the resource usage of the bulk indexing method of Elasticsearch.
Throttling is possible by limiting the number of bulk actions per request or by the
maximum number of concurrent request.

The `Sink` API can be inspected at 
http://jprante.github.io/elasticsearch-jdbc/apidocs/org/xbib/elasticsearch/jdbc/strategy/Sink.html

### Context

The `Context` is the abstraction to the thread which performs data fetching from the source 
and transports it to the mouth. A 'move' is considered a single step in the execution cycle. 

The `Context` API can be inspected at 
http://jprante.github.io/elasticsearch-jdbc/apidocs/org/xbib/elasticsearch/jdbc/strategy/Context.html



# Examples

## Oracle column name 30 character limit

Oracle imposes a 30 character limit on column name aliases. This makes it sometimes hard to define columns names for
Elasticsearch field names. For this, a column name map can be used like this:

    {
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
    }

## Connection properties for JDBC driver

For some JDBC drivers, advanced parameters can be passed that are not specified in the driver URL, 
but in the JDBC connection properties. You can specifiy connection properties like this:

    {
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
    }


# License

Elasticsearch JDBC Importer

Copyright (C) 2012-2015 Jörg Prante

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
