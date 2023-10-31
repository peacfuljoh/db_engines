
import os
import datetime


"""MySQL info"""
try:
    import json

    GLOBAL_CONFIG_PATH = '/home/nuc/crawler_config/config.json'
    with open(GLOBAL_CONFIG_PATH, 'r') as f:
        config = json.load(f)

    # config info
    DB_MYSQL_CONFIG = config['DB_CONFIG']
    DB_MONGO_CONFIG = config['DB_MONGO_CONFIG']
except:
    DB_MYSQL_CONFIG = dict(
        host="localhost",
        user=os.environ['MYSQL_USERNAME'],
        password=os.environ['MYSQL_PASSWORD']
    )

DATABASES_MYSQL = dict(
    test='test852943'
)

TABLENAMES_MYSQL = {
    DATABASES_MYSQL['test']: dict(
        usernames='usernames',
        meta='meta',
        stats='stats'
    ),
}

SCHEMA_SQL_FNAME = 'schema_test.sql'


CMDS_INSERT_MYSQL = dict(
    usernames="""
                INSERT INTO usernames
                (username)
                VALUES (%s)
                """,
    meta="""
                INSERT INTO meta
                (id_meta, username, date_meta, timestamp_meta, score)
                VALUES (%s, %s, %s, %s, %s)
                """,
    stats="""
                INSERT INTO stats
                (id_meta, count_stats, text_stats, timestamp_stats)
                VALUES (%s, %s, %s, %s)
                """
)
DATA_INSERT_MYSQL = dict(
    usernames=[('blah',), ('test',), ('here',)],
    meta=[('123', 'blah', datetime.date(2020, 2, 2), datetime.datetime(2020, 2, 2, 10, 5, 3), 50),
          ('765', 'test', datetime.date(2022, 1, 5), datetime.datetime(2022, 1, 5, 4, 27, 11), 210)],
    stats=[('123', 5454, 'some text', datetime.datetime(2020, 3, 1, 13, 5, 3)),
           ('123', 6532, 'some more text', datetime.datetime(2020, 6, 7, 23, 2, 5))]
)

TABLE_COLS_MYSQL = dict(
    usernames=['username'],
    meta=['id_meta', 'username', 'date_meta', 'timestamp_meta', 'score'],
    stats=['id_meta', 'count_stats', 'text_stats', 'timestamp_stats']
)

TABLE_COLS_PRI_MYSQL = dict(
    usernames=['username'],
    meta=['id_meta'],
    stats=['id_meta', 'timestamp_stats']
)

