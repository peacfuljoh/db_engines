"""Utils implementing useful ops over MySQL engine."""

from typing import List, Optional, Dict, Tuple, Generator

import pandas as pd
from ytpa_utils.sql_utils import make_sql_query_where_one
from ytpa_utils.val_utils import is_subset

from .mysql_engine import MySQLEngine



def get_table_colnames(database: str,
                       tablename: str,
                       db_config: dict) \
        -> List[str]:
    """Get list of column names for a table in a database"""
    engine = MySQLEngine(db_config)
    table_info = engine.describe_table(database, tablename)
    colnames = [tup[0] for tup in table_info]
    return colnames


def get_table_primary_keys(database: str,
                           tablename: str,
                           db_config: dict) \
        -> List[str]:
    """Get list of primary-key column names for a table in a database"""
    engine = MySQLEngine(db_config)
    table_info = engine.describe_table(database, tablename)
    colnames = [tup[0] for tup in table_info if tup[3] == 'PRI']
    return colnames


def prep_keys_for_insert_or_update(database: str,
                                   tablename: str,
                                   data: dict,
                                   db_config: dict,
                                   keys: Optional[List[str]] = None) \
        -> List[str]:
    """Validation and prep of keys for insert and update ops"""
    # if keys not specified, get all column names for this table
    if keys is None:
        keys = get_table_colnames(database, tablename, db_config)

    # key list is non-empty
    assert len(keys) > 0

    # keys is a subset of dict keys
    is_subset(keys, data)

    # if multiple records, must have same number of records for all keys
    if isinstance(data[keys[0]], list):
        lens = [len(e) for e in data.values()]
        assert all([len_ == lens[0] for len_ in lens])

    return keys


def insert_records_from_dict(database: str,
                             tablename: str,
                             data: dict,
                             db_config: dict,
                             keys: Optional[List[str]] = None):
    """
    Insert all or a subset of the info from a dict to a database table. Subset is specified through 'keys' arg.

    Data dict could have individual entries or a list for each key. In the latter case, the number of entries must
    match for all keys.

    On duplicate key, do nothing.

    INSERT INTO table_name (column1, column2, column3, ...)
    VALUES (value1, value2, value3, ...)
    ON DUPLICATE KEY UPDATE column1=value1, ...;
    """
    keys = prep_keys_for_insert_or_update(database, tablename, data, db_config, keys=keys)

    query = f"INSERT INTO {tablename} ({','.join(keys)}) VALUES (" + ','.join(['%s'] * len(keys)) + ")"
    query += f" ON DUPLICATE KEY UPDATE {keys[0]}={keys[0]}"

    if isinstance(data[keys[0]], list):
        records: List[tuple] = [tuple([data[key][i] for key in keys]) for i in range(len(data[keys[0]]))]
    else:
        records: List[tuple] = [tuple([data[key] for key in keys])]

    engine = MySQLEngine(db_config)
    engine.insert_records_to_table(database, query, records)

    a = 5


def update_records_from_dict(database: str,
                             tablename: str,
                             data: dict,
                             db_config: dict,
                             condition_keys: Optional[List[str]] = None,
                             keys: Optional[List[str]] = None,
                             another_condition: Optional[str] = None):
    """
    Same as insert_records_from_dict() but applying an update operation.

    UPDATE table_name
    SET column1 = value1, column2 = value2, ...
    WHERE condition;

    'condition_keys' are the columns that are used in the WHERE clause (which records to update)
    'keys' are the columns to be updated
    """
    keys = prep_keys_for_insert_or_update(database, tablename, data, db_config, keys=keys)

    if condition_keys is None:
        condition_keys = get_table_primary_keys(database, tablename, db_config)
    assert len(condition_keys) > 0
    is_subset(condition_keys, data)
    # assert len(set(condition_keys) - set(raw_data.keys())) == 0

    query = f"UPDATE {tablename} SET " + ', '.join([key + ' = %s' for key in keys])
    query += ' WHERE ' + ' AND '.join([key + ' = %s' for key in condition_keys])
    if another_condition is not None:
        query += ' AND ' + another_condition

    if isinstance(data[keys[0]], list):
        records: List[tuple] = [
            tuple(
                [data[key][i] for key in keys] + [data[key][i] for key in condition_keys]
            )
            for i in range(len(data[keys[0]]))
        ]
    else:
        records: List[tuple] = [tuple([data[key] for key in keys] + [data[key] for key in condition_keys])]

    engine = MySQLEngine(db_config)
    engine.insert_records_to_table(database, query, records)


def perform_join_mysql_query(db_config: dict,
                             database: str,
                             tablename_primary: str,
                             tablename_secondary: str,
                             table_pseudoname_primary: str,
                             table_pseudoname_secondary: str,
                             join_condition: str,
                             cols_all: Dict[str, List[str]],
                             filters: Optional[dict] = None,
                             limit: Optional[int] = None,
                             as_generator: bool = False) \
        -> Tuple[Generator[pd.DataFrame, None, None], MySQLEngine]:
    """
    Perform join query with specified options.

    Args:
        database: name of database to perform query on
        tablename_primary: first table in join
        tablename_secondary: second table in join
        table_pseudoname_primary: post-join name for first table
        table_pseudoname_secondary: post-join name for second table
        join_condition: join condition clause in query
        cols_all: dict with columns from the two tables to return (keys are table
                  pseudonames and keys are lists of corresponding column names)
        filters: dict with conditions for WHERE clause, e.g. filters = dict(username=['uname1', 'uname2']). See all
                 options for specifying sub-clauses. All sub-clauses are AND'd together.
        limit: max number of records to return
    """
    # column info for query
    cols_for_query = ([f'{table_pseudoname_primary}.{colname}' for colname in cols_all[table_pseudoname_primary]] +
                      [f'{table_pseudoname_secondary}.{colname}' for colname in cols_all[table_pseudoname_secondary]])
    cols_for_df = cols_all[table_pseudoname_primary] + cols_all[table_pseudoname_secondary]

    # where clause
    where_clause = None

    if filter is not None and len(filters) > 0:
        where_clauses = []
        for key, val in filters.items():
            # identify table that this condition applies to
            tablename_ = [tname for tname, colnames_ in cols_all.items() if key in colnames_]
            assert len(tablename_) == 1
            tablename_ = tablename_[0]

            # add where clause
            where_clauses.append(make_sql_query_where_one(tablename_, key, val))

        # join sub-clauses
        where_clause = ' AND '.join(where_clauses)

    # issue request
    engine = MySQLEngine(db_config)
    df = engine.select_records_with_join(
        database,
        tablename_primary,
        tablename_secondary,
        join_condition,
        cols_for_query,
        table_pseudoname_primary=table_pseudoname_primary,
        table_pseudoname_secondary=table_pseudoname_secondary,
        where_clause=where_clause,
        limit=limit,
        cols_for_df=cols_for_df,
        as_generator=as_generator
    )

    return df, engine
