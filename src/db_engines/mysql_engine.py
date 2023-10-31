"""
Database utils for MySQL including basic CRUD operations and more
complex functionality using the engine.
"""

from typing import Dict, Optional, Callable, List, Union, Generator

import mysql.connector
import pandas as pd

from .constants import MYSQL_FETCH_MANY_MAX_COUNT




class MySQLEngine():
    """MySQL convenience class for CRUD and other operations on database records."""
    def __init__(self, db_config: Dict[str, str]):
        # members
        self._db_config = None

        # setup
        self.set_db_config(db_config)


    ### connection config ###
    def set_db_config(self, db_config: Dict[str, str]):
        self._db_config = db_config

    def get_db_config(self) -> Dict[str, str]:
        return self._db_config


    ### connection and exception handling ###
    def _get_connection(self, database: Optional[str] = None):
        """Establish connection with a MySQL database"""
        return mysql.connector.connect(
            host=self._db_config['host'],
            user=self._db_config['user'],
            password=self._db_config['password'],
            database=database
        )

    def _sql_query_wrapper(self,
                           func: Callable,
                           database: Optional[str] = None):
        """Wrapper for exception handling during MySQL queries"""
        try:
            with self._get_connection(database=database) as connection:
                with connection.cursor() as cursor:
                    return func(connection, cursor)
        except mysql.connector.Error as e:
            print(e)


    ### get database and table info ###
    def get_db_names(self) -> List[str]:
        """Get all names of existing databases"""
        def func(connection, cursor):
            cursor.execute("SHOW DATABASES")
            return [db_name[0] for db_name in cursor]
        return self._sql_query_wrapper(func)

    def describe_table(self,
                       database: str,
                       tablename: str) \
            -> List[tuple]:
        """Return schema of a table"""
        def func(connection, cursor):
            cursor.execute(f"DESCRIBE {tablename}")
            return cursor.fetchall()
        return self._sql_query_wrapper(func, database=database)



    ### pure SQL ###
    def execute_pure_sql(self,
                         database: str,
                         query: str):
        def func(connection, cursor):
            cursor.execute(query)
            connection.commit()

        self._sql_query_wrapper(func, database=database)



    ### operations on databases ###
    def create_db(self, db_name: str):
        """Create a database"""
        def func(connection, cursor):
            cursor.execute(f"CREATE DATABASE {db_name}")
            connection.commit()
        return self._sql_query_wrapper(func)

    def create_db_from_sql_file(self, filename: str):
        """Create a database from a .sql file with 'CREATE TABLE IF NOT EXISTS ...' statements"""
        assert '.sql' in filename
        def func(connection, cursor):
            with open(filename, 'r') as fd:
                sqlFile = fd.read()
            sqlCommands = sqlFile.split(';')
            for command in sqlCommands:
                if command.strip() != '':
                    cursor.execute(command)
            # with open(filename, 'r') as f:
            #     cursor.execute(f.read(), multi=True) # doesn't work...?
            connection.commit()
        return self._sql_query_wrapper(func)

    def drop_db(self, db_name: str):
        """Delete a database"""
        def func(connection, cursor):
            cursor.execute(f"DROP DATABASE {db_name}")
            connection.commit()
        return self._sql_query_wrapper(func)


    ### operations on tables ###
    def create_tables(self,
                      database: str,
                      queries: Union[str, List[str]]):
        """Create one or more tables in a specified database"""
        def func(connection, cursor):
            if isinstance(queries, str):
                cursor.execute(queries)
                connection.commit()
            else:
                for query in queries:
                    cursor.execute(query)
                    connection.commit()
        return self._sql_query_wrapper(func, database=database)

    def insert_records_to_table(self,
                                database: str,
                                query: str,
                                records: Optional[Union[str, List[tuple]]] = None):
        """Insert records into a table using a single query or a split query (instructions + raw_data)"""
        def func(connection, cursor):
            if records is None:
                cursor.execute(query)
            else:
                cursor.executemany(query, records)
            connection.commit()
        return self._sql_query_wrapper(func, database=database)

    def select_records(self,
                       database: str,
                       query: str,
                       mode: str = 'list',
                       tablename: Optional[str] = None,
                       cols: Optional[List[str]] = None,
                       as_generator: bool = False) \
            -> Union[Generator[pd.DataFrame, None, None], Generator[List[tuple], None, None], pd.DataFrame, List[tuple]]:
        """
        Retrieve records from a table.

        If mode == 'pandas', you must specify either the tablename (to infer all of that table's column names) or
        cols, which is the list of columns that the query will return. Either way, these column names will be used
        as the column names in the returned pandas dataframe.
        """
        assert mode in ['list', 'pandas']
        if mode == 'pandas':
            assert (tablename is None and cols is not None) or (tablename is not None and cols is None)
            if cols is None:
                cols = [e[0] for e in self.describe_table(database, tablename)]
        if mode == 'list':
            assert tablename is None

        if not as_generator:
            def func(connection, cursor):
                cursor.execute(query)
                records = cursor.fetchall() # if table empty, "1241 (21000): Operand should contain 1 column(s)"
                if mode == 'pandas':
                    return pd.DataFrame(records, columns=cols)
                return records

            return self._sql_query_wrapper(func, database=database)
        else:
            return self._select_records_gen(database, query, mode, cols=cols)

    def _select_records_gen(self,
                            database: str,
                            query: str,
                            mode: str = 'list',
                            cols: Optional[List[str]] = None):
        # sql_query_wrapper() doesn't work with yield...
        # Throws `mysql.connector.errors.ProgrammingError: 2055: Cursor is not connected`
        try:
            with self._get_connection(database=database) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    while (records := cursor.fetchmany(MYSQL_FETCH_MANY_MAX_COUNT)) is not None:
                        if not records:
                            return
                        if mode == 'pandas':
                            yield pd.DataFrame(records, columns=cols)
                        else:
                            yield records
        except mysql.connector.Error as e:
            print(e)

        # def func(connection, cursor):
        #     cursor.execute(query)
        #     while (records := cursor.fetchmany(100)) is not None:
        #         if mode == 'pandas':
        #             yield pd.DataFrame(records, columns=cols)
        #         else:
        #             yield records
        # return self._sql_query_wrapper(func, database=database)



    def select_records_with_join(self,
                                 database: str,
                                 tablename_primary: str,
                                 tablename_secondary: str,
                                 join_condition: str,
                                 cols_for_query: List[str],
                                 table_pseudoname_primary: Optional[str] = None,
                                 table_pseudoname_secondary: Optional[str] = None,
                                 where_clause: Optional[str] = None,
                                 limit: Optional[int] = None,
                                 cols_for_df: Optional[List[str]] = None,
                                 as_generator: bool = False) \
            -> Union[Generator[pd.DataFrame, None, None], pd.DataFrame]:
        """Select query on one table joined on second table"""
        if table_pseudoname_primary is None:
            table_pseudoname_primary = tablename_primary
        if table_pseudoname_secondary is None:
            table_pseudoname_secondary = tablename_secondary
        if cols_for_df is None:
            cols_for_df = cols_for_query
        query = (f"SELECT {', '.join(cols_for_query)} FROM {tablename_primary} as {table_pseudoname_primary} "
                 f"JOIN {tablename_secondary} as {table_pseudoname_secondary} ON {join_condition}")
        if where_clause is not None:
            query += f" WHERE {where_clause}"
        if limit is not None:
            query += f" LIMIT {limit}"

        return self.select_records(database, query, mode='pandas', cols=cols_for_df, as_generator=as_generator)
