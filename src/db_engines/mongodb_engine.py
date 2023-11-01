"""MongoDB Engine for CRUD and other ops"""

from typing import Dict, Union, Optional, Callable, List, Generator
import math

import pandas as pd

from pymongo import MongoClient
from pymongo.collection import Collection, ObjectId, Cursor
from pymongo.errors import BulkWriteError

from ytpa_utils.val_utils import is_list_of_instances

from .constants import MONGODB_FIND_MANY_MAX_COUNT



class MongoDBEngine():
    """Convenience class for interactions with a MongoDB database"""
    def __init__(self,
                 db_config: Dict[str, Union[str, int]],
                 database: Optional[str] = None,
                 collection: Optional[str] = None,
                 verbose: bool = False):
        self._db_config = db_config
        self._database = None
        self._collection = None
        self._verbose = verbose

        self.set_db_info(database=database, collection=collection)

        self._db_client = MongoClient(self._db_config['host'], self._db_config['port'])

    def __del__(self):
        self._db_client.close()

    def set_db_info(self,
                    database: Optional[str] = None,
                    collection: Optional[str] = None):
        """Set database and collection to be used in db op calls"""
        if database is not None:
            self._database = database
        if collection is not None:
            self._collection = collection

    def get_db_info(self):
        """Get currently targeted database and collection"""
        return self._database, self._collection


    ### connection config ###
    def get_db_config(self) -> Dict[str, str]:
        return self._db_config


    ## connection and exception handling ###
    def _query_wrapper(self, func: Callable):
        """Wrapper for exception handling during MongoDB queries"""
        try:
            return func()
        except Exception as e:
            print(e)


    ## DB inspection ##
    def get_all_databases(self) -> List[str]:
        """Get all databases"""
        return self._db_client.list_database_names()

    def get_all_collections(self, database: Optional[str] = None) -> Dict[str, List[str]]:
        """Get all collections by database or just those for a specified database"""
        if database is not None:
            databases = [database]
        else:
            databases = self.get_all_databases()
        return {database: self._db_client[database].list_collection_names() for database in databases}

    def get_ids(self) -> List[str]:
        """Get all IDs for a collection"""
        # TODO: implement exception efficiently, e.g. write ids to temp collection, then read from there:
        #   https://stackoverflow.com/questions/27323107/mongodb-distinct-too-big-16mb-cap
        try:
            return [str(id) for id in self._get_collection().distinct('_id')]
        except:
            # exception if distinct is too large
            df_gen = self.find_many_gen(projection={'_id': 1})
            ids = [str(id_) for df_ in df_gen for id_ in df_['_id']]
            return ids

    ## DB operations ##
    def _get_collection(self) -> Collection:
        """Get collection object for queries"""
        assert self._database is not None
        assert self._collection is not None
        return self._db_client[self._database][self._collection]

    def insert_one(self, record: dict):
        """Insert one record"""
        def func():
            cn = self._get_collection()

            if ('_id' in record) and (cn.find_one({"_id": record['_id']}) is not None):
                raise Exception(f'MongoDBEngine: A record with id {record["_id"]} already exists in collection '
                                f'{self._collection} of database {self._database}.')

            res = cn.insert_one(record)

            if self._verbose:
                print(f'MongoDBEngine: Inserted {1} record with id {res.inserted_id} in collection {self._collection} '
                      f'of database {self._database}.')

        return self._query_wrapper(func)

    def insert_many(self, records: List[dict]):
        """Insert many records"""
        def func():
            try:
                cn = self._get_collection()
                cn.insert_many(records, ordered=False)
            except BulkWriteError as e:
                if self._verbose:
                    writeErrors = e.details['writeErrors']
                    print(f"Failed to write {len(writeErrors)} out of {len(records)} records.")
        return self._query_wrapper(func)

    def update_one(self,
                   filter: dict,
                   update: dict,
                   upsert: bool = False):
        """Update a single record"""
        def func():
            cn = self._get_collection()
            cn.update_one(filter, update, upsert=upsert)
        return self._query_wrapper(func)

    def update_many(self,
                    filter: dict,
                    update: List[dict],
                    upsert: bool = False,
                    max_pipeline_len: Optional[int] = 1000):
        """Update records"""
        def func():
            cn = self._get_collection()
            num_pipelines = math.ceil(len(update) / max_pipeline_len)
            for i in range(num_pipelines):
                update_i = update[i * max_pipeline_len: (i + 1) * max_pipeline_len]
                if self._verbose:
                    print(f'Updating {len(update_i)} records.')
                cn.update_many(filter, update_i, upsert=upsert)
        return self._query_wrapper(func)

    def find_one_by_id(self, id: str) -> Optional[dict]:
        """Find a single record"""
        def func():
            cn = self._get_collection()

            # try provided id as-is
            rec = cn.find_one({"_id": id})
            if rec is not None:
                return rec

            # try converting to ObjectId
            rec = cn.find_one({"_id": ObjectId(id)})
            if rec is not None:
                return rec

            # fail
            raise Exception(f'Could not find record with _id {id}.')

        return self._query_wrapper(func)

    def find_one(self,
                 filter: Optional[dict] = None,
                 projection: Optional[dict] = None) \
            -> dict:
        """Same as self.find_many_gen() but for a single record."""
        if filter is None:
            filter = {}

        def func():
            cn = self._get_collection()
            if projection is None:
                cursor = cn.find(filter, limit=1)
            else:
                cursor = cn.find(filter, projection, limit=1)
            return next(cursor, None)
        return self._query_wrapper(func)

    def find_many_by_ids(self,
                         ids: Optional[List[str]] = None,
                         limit: int = 0,
                         filter_other: Optional[dict] = None) \
            -> List[dict]:
        """Find many records"""
        def func():
            cn = self._get_collection()
            filter = {} if ids is None else {"_id": {"$in": ids}}
            if filter_other is not None:
                filter = {**filter, **filter_other}
            cursor = cn.find(filter, limit=limit)
            return [d for d in cursor]
        return self._query_wrapper(func)

    def find_many_gen(self,
                      filter: Optional[dict] = None,
                      projection: Optional[dict] = None) \
            -> Generator[pd.DataFrame, None, None]:
        """
        Generator of records given optional filter and projection arguments.

        Args:
        - 'filter' is a dict with options analogous to a WHERE clause in a SQL query.
            e.g. filter = dict(name={'$in': ['a', 'b']}).
        - 'projection' is a dict with fields to return, analogous to "SELECT item, status FROM ..."
          instead of "SELECT * FROM ..." in a SQL query:
            e.g. {"item": 1, "status": 1, "_id": 0} # this drops '_id' in the returned dict
        """
        if filter is None:
            filter = {}

        def func():
            cn = self._get_collection()
            if projection is None:
                cursor = cn.find(filter)
            else:
                cursor = cn.find(filter, projection)
            return df_generator(cursor)

        return self._query_wrapper(func)

    def find_with_group_gen(self,
                            group: dict,
                            filter: Optional[dict] = None) \
            -> Generator[pd.DataFrame, None, None]:
        """Find records using an aggregation pipeline"""
        def func():
            cn = self._get_collection()

            pipeline = []
            if filter is not None:
                pipeline += [filter]
            pipeline += [{"$group": group}]

            cursor = cn.aggregate(pipeline)
            return df_generator(cursor)

        return self._query_wrapper(func)

    def find_distinct_gen(self,
                          field: str,
                          filter: Optional[dict] = None) \
            -> Generator[pd.DataFrame, None, None]:
        """
        Find all distinct values of a given field.
        Output is a generator of DataFrames that have one column whose label is the field input arg.

        e.g. filter = {'$match': {<key1>: <one_val>, <key2>: {'$in': <list_of_vals>}}}
        """
        assert filter is None or (len(filter) == 1 and '$match' in filter)
        def func():
            group = {"_id": "$" + field}
            for df in self.find_with_group_gen(group, filter=filter):
                yield df.rename(columns={'_id': field})
        return self._query_wrapper(func)

    def delete_many(self, ids: Union[List[str], dict]):
        """Delete records by id"""
        assert is_list_of_instances(ids, (str, ObjectId)) or ids == {}
        def func():
            cn = self._get_collection()
            filter = {"_id": {"$in": ids}} if isinstance(ids, list) else {}
            cn.delete_many(filter)
        return self._query_wrapper(func)

    def delete_all_records(self, confirm_delete: Optional[str] = None):
        """Delete all records in a collection"""
        if confirm_delete != 'yes':
            return
        def func():
            cn = self._get_collection()
            cn.delete_many({})
        return self._query_wrapper(func)


""" Helper methods """
def df_generator(cursor: Cursor) -> Generator[pd.DataFrame, None, None]:
    """Generator of DataFrames from records produced by iterating on a PyMongo cursor"""
    while 1:
        recs: List[dict] = []
        for _ in range(MONGODB_FIND_MANY_MAX_COUNT):
            rec_ = next(cursor, None)
            if rec_ is None:
                break
            recs.append(rec_)
        if recs:
            yield pd.DataFrame(recs)
        else:
            return
