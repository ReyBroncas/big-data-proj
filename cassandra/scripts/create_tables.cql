CREATE KEYSPACE IF NOT EXISTS big_data_project
    WITH REPLICATION = {
        'class' : 'SimpleStrategy', 'replication_factor' : 1
    };

USE big_data_project;

CREATE TABLE IF NOT EXISTS table_1 (
    date timestamp,
    domain text,
    page_title text,
    user_id text,
    is_bot int,
    user_name text,
    PRIMARY KEY (domain, date, is_bot, user_id)
) WITH CLUSTERING ORDER BY (date DESC, is_bot DESC);

CREATE TABLE IF NOT EXISTS table_2 (
    user_id text,
    user_name text,
    page_title text,
    date timestamp,
    PRIMARY KEY (user_id, date, page_title));

