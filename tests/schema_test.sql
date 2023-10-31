CREATE DATABASE IF NOT EXISTS test852943;
USE test852943;
CREATE TABLE IF NOT EXISTS usernames (
    username VARCHAR(50) PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS meta (
    id_meta VARCHAR(20) PRIMARY KEY,
    username VARCHAR(50),
    date_meta DATE,
    timestamp_meta TIMESTAMP(3),
    score SMALLINT UNSIGNED
);
CREATE TABLE IF NOT EXISTS stats (
    id_meta VARCHAR(20),
    count_stats INT UNSIGNED,
    text_stats VARCHAR(100),
    timestamp_stats TIMESTAMP(3),
    FOREIGN KEY(id_meta) REFERENCES meta(id_meta),
    PRIMARY KEY(id_meta, timestamp_stats)
);