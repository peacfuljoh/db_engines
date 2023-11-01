
import os
import datetime


try: # local
    import json

    # MySQL
    GLOBAL_CONFIG_PATH = '/home/nuc/crawler_config/config.json'
    with open(GLOBAL_CONFIG_PATH, 'r') as f:
        config = json.load(f)

    # config info
    DB_MYSQL_CONFIG = config['DB_CONFIG']
    DB_MONGO_CONFIG = config['DB_MONGO_CONFIG']

    # MongoDB
except: # CI/CD
    # MySQL
    DB_MYSQL_CONFIG = dict(
        host="localhost",
        user=os.environ['MYSQL_USERNAME'],
        password=os.environ['MYSQL_PASSWORD']
    )

    # MongoDB
    DB_MONGO_CONFIG = dict(
        host=os.environ['MONGODB_HOST'],
        port=os.environ['MONGODB_PORT']
    )


"""MySQL"""
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



"""MongoDB"""
DATABASES_MONGODB = dict(
    test1='test852943',
    test2='test754637'
)

COLLECTIONS_MONGODB = {
    DATABASES_MONGODB['test1']: dict(
        test11='test_collection11',
        test12='test_collection12'
    ),
    DATABASES_MONGODB['test2']: dict(
        test21='test_collection21',
        test22='test_collection22'
    ),
}
