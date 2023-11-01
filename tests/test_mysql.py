"""Tests for the MySQL engine"""

from typing import List, Optional
import datetime
import os

import pandas as pd

from src.db_engines.mysql_engine import MySQLEngine
from src.db_engines.mysql_utils import (get_table_colnames, get_table_primary_keys, insert_records_from_dict,
                                        update_records_from_dict)
from utils_tests import (DB_MYSQL_CONFIG, DATABASES_MYSQL, TABLENAMES_MYSQL, SCHEMA_SQL_FNAME,
                         CMDS_INSERT_MYSQL, DATA_INSERT_MYSQL, TABLE_COLS_MYSQL, TABLE_COLS_PRI_MYSQL)


DB_TEST = DATABASES_MYSQL['test']



""" Helper methods """
def setup_test_db(engine: MySQLEngine,
                  inject_data: bool = False):
    """Create test database. Overwrite if it already exists."""
    if DB_TEST in engine.get_db_names():
        engine.drop_db(DB_TEST)

    schema_fname = SCHEMA_SQL_FNAME if os.path.exists(SCHEMA_SQL_FNAME) else os.path.join('tests', SCHEMA_SQL_FNAME)
    assert os.path.exists(schema_fname)
    engine.create_db_from_sql_file(schema_fname)

    if inject_data:
        for tablename, cmd in CMDS_INSERT_MYSQL.items():
            engine.insert_records_to_table(DB_TEST, cmd, DATA_INSERT_MYSQL[tablename])

def convert_df_rec_to_list(df: pd.DataFrame,
                           tablename: Optional[str] = None,
                           cols: Optional[List[str]] = None) \
        -> List[tuple]:
    """Convert DataFrame returned from MySQL engine to list-of-tuples format."""
    assert tablename is None or cols is None
    if tablename is not None:
        cols = TABLE_COLS_MYSQL[tablename]
    return [tuple([rec[key] for key in cols if key in rec]) for rec in df.to_dict('records')]




""" MySQLEngine Tests """
def test_engine_setup():
    # start engine
    engine = MySQLEngine(DB_MYSQL_CONFIG)

    # check config and connection
    assert engine.get_db_config() == DB_MYSQL_CONFIG
    with engine._get_connection() as cn:
        assert cn.is_connected()

    # create database
    setup_test_db(engine)
    assert DB_TEST in engine.get_db_names()

    # check connection with database
    with engine._get_connection(database=DB_TEST) as cn:
        assert cn.is_connected()
        assert cn.database == DB_TEST

def test_describe_table():
    engine = MySQLEngine(DB_MYSQL_CONFIG)
    setup_test_db(engine)

    exp = dict(
        usernames=[('username', 'varchar(50)', 'NO', 'PRI', None, '')],
        meta=[('id_meta', 'varchar(20)', 'NO', 'PRI', None, ''),
              ('username', 'varchar(50)', 'YES', '', None, ''),
              ('date_meta', 'date', 'YES', '', None, ''),
              ('timestamp_meta', 'timestamp(3)', 'YES', '', None, ''),
              ('score', 'smallint unsigned', 'YES', '', None, '')],
        stats=[('id_meta', 'varchar(20)', 'NO', 'PRI', None, ''),
               ('count_stats', 'int unsigned', 'YES', '', None, ''),
               ('text_stats', 'varchar(100)', 'YES', '', None, ''),
               ('timestamp_stats', 'timestamp(3)', 'NO', 'PRI', None, '')]
    )

    for tablename in TABLENAMES_MYSQL[DB_TEST]:
        desc = engine.describe_table(DB_TEST, tablename)
        for tup_out, tup_exp in zip(desc, exp[tablename]):
            for val_out, val_exp in zip(tup_out, tup_exp):
                assert (val_out == val_exp or
                        (isinstance(val_exp, bytes) and bytes(val_out, encoding='utf-8') == val_exp))

def test_select_records():
    engine = MySQLEngine(DB_MYSQL_CONFIG)
    setup_test_db(engine, inject_data=True)

    for tablename in TABLENAMES_MYSQL[DB_TEST]:
        expected = set(DATA_INSERT_MYSQL[tablename])

        # list
        recs = engine.select_records(DB_TEST, f"SELECT * FROM {tablename}")
        assert set(recs) == expected

        # DataFrame
        df = engine.select_records(DB_TEST, f"SELECT * FROM {tablename}", mode='pandas', tablename=tablename)
        assert set(convert_df_rec_to_list(df, tablename=tablename)) == expected

        # generator
        df_gen = engine.select_records(DB_TEST, f"SELECT * FROM {tablename}", mode='pandas', tablename=tablename,
                                       as_generator=True)
        dfs = [df_ for df_ in df_gen]
        assert len(dfs) == 1
        assert set(convert_df_rec_to_list(dfs[0], tablename=tablename)) == expected

# def test_execute_pure_sql():
#     # TODO: execute_pure_sql doesn't seem to like these test cases, gives "Unread result found"
#     if 0:
#         engine = MySQLEngine(DB_MYSQL_CONFIG)
#         setup_test_db(engine, inject_data=True)
#
#         cmds = [
#             'SELECT id_meta, username FROM meta ORDER BY username',
#             'SELECT * FROM usernames ORDER BY username'
#         ]
#         exps = [
#             [('123', 'blah'), ('765', 'test')],
#             [('blah',), ('here',)]
#         ]
#
#         for cmd_, exp_ in zip(cmds, exps):
#             assert engine.execute_pure_sql(DB_TEST, cmd_) == exp_

def test_select_records_with_join():
    engine = MySQLEngine(DB_MYSQL_CONFIG)
    setup_test_db(engine, inject_data=True)

    tablename_primary = 'meta'
    tablename_secondary = 'stats'
    table_pseudoname_primary = 'm'
    table_pseudoname_secondary = 's'

    join_condition = f"{table_pseudoname_primary}.id_meta = {table_pseudoname_secondary}.id_meta"

    cols_all = {
        table_pseudoname_primary: ['id_meta', 'username'],
        table_pseudoname_secondary: ['count_stats']
    }

    cols_for_query = ([f'{table_pseudoname_primary}.{colname}' for colname in cols_all[table_pseudoname_primary]] +
                      [f'{table_pseudoname_secondary}.{colname}' for colname in cols_all[table_pseudoname_secondary]])
    cols_for_df = cols_all[table_pseudoname_primary] + cols_all[table_pseudoname_secondary]

    where_clause = f"{table_pseudoname_secondary}.count_stats > 6000"
    limit = 10
    as_generator = False

    df = engine.select_records_with_join(
        DB_TEST,
        tablename_primary,
        tablename_secondary,
        join_condition,
        cols_for_query,
        table_pseudoname_primary,
        table_pseudoname_secondary,
        where_clause,
        limit,
        cols_for_df,
        as_generator
    )
    rec_ = convert_df_rec_to_list(df, cols=['id_meta', 'username', 'count_stats'])

    assert set(rec_) == set([('123', 'blah', 6532)])





""" MySQL utils tests """
def test_get_table_colnames():
    engine = MySQLEngine(DB_MYSQL_CONFIG)
    setup_test_db(engine, inject_data=True)

    for tablename, colnames_exp in TABLE_COLS_MYSQL.items():
        colnames = get_table_colnames(DB_TEST, tablename, DB_MYSQL_CONFIG)
        assert set(colnames_exp) == set(colnames)

def test_get_table_primary_keys():
    engine = MySQLEngine(DB_MYSQL_CONFIG)
    setup_test_db(engine, inject_data=True)

    for tablename, keys_exp in TABLE_COLS_PRI_MYSQL.items():
        keys = get_table_primary_keys(DB_TEST, tablename, DB_MYSQL_CONFIG)
        assert set(keys_exp) == set(keys)

def test_insert_and_update_records_from_dict():
    """Insert"""
    engine = MySQLEngine(DB_MYSQL_CONFIG)
    setup_test_db(engine, inject_data=True)

    # insert to meta (necessary for foreign key from stats table)
    tablename = 'meta'
    data_meta = dict(
        id_meta='444'
    )
    keys = ['id_meta']
    insert_records_from_dict(DB_TEST, tablename, data_meta, DB_MYSQL_CONFIG, keys=keys)

    # insert into stats table
    tablename = 'stats'
    data_stats = dict(
        id_meta=['444', '444'],
        # count_stats=[11, 22],
        text_stats=['bbbb', 'ccc'],
        timestamp_stats=[
            datetime.datetime(2022, 3, 1, 13, 5, 3),
            datetime.datetime(2023, 6, 7, 23, 2, 5)
        ]
    )
    keys = ['id_meta', 'text_stats', 'timestamp_stats']
    # keys = None
    insert_records_from_dict(DB_TEST, tablename, data_stats, DB_MYSQL_CONFIG, keys=keys)

    # select the added records by id_meta and compare to expected
    recs = engine.select_records(DB_TEST, f"SELECT * FROM {tablename} WHERE id_meta = '444'")

    expected = [('444', None, 'bbbb', datetime.datetime(2022, 3, 1, 13, 5, 3)),
                ('444', None, 'ccc', datetime.datetime(2023, 6, 7, 23, 2, 5))]

    assert set(recs) == set(expected)

    """Update"""
    data_stats_update = dict(
        id_meta='444',
        timestamp_stats=datetime.datetime(2023, 6, 7, 23, 2, 5),
        count_stats=9
    )
    keys = ['count_stats']
    condition_keys = ['id_meta', 'timestamp_stats']

    update_records_from_dict(DB_TEST, tablename, data_stats_update, DB_MYSQL_CONFIG, condition_keys=condition_keys,
                             keys=keys)#, another_condition=another_condition)

    # select the updated records by id_meta and compare to expected
    recs = engine.select_records(DB_TEST, f"SELECT * FROM {tablename} WHERE id_meta = '444'")

    expected = [('444', None, 'bbbb', datetime.datetime(2022, 3, 1, 13, 5, 3)),
                ('444', 9, 'ccc', datetime.datetime(2023, 6, 7, 23, 2, 5))]

    assert set(recs) == set(expected)


