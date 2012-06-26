use test;

drop table if exists my_jdbc_river;

create table my_jdbc_river (
    `_index` varchar(64),
    `_type` varchar(64),
    `_id` varchar(64),
    `source_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `source_operation` varchar(8),
    `source_sql` varchar(255),
    `target_timestamp` timestamp,
    `target_operation` varchar(8) default 'n/a',
    `target_failed` boolean,
    `target_message` varchar(255),
    primary key (`_index`, `_type`, `_id`, `source_timestamp`, `source_operation`)
);
