"""Tests for MongoDB engine and other utils"""

from typing import Dict, List, Tuple

import pandas as pd

from src.db_engines.mongodb_engine import MongoDBEngine
from src.db_engines.mongodb_utils import get_mongodb_records_gen, load_all_recs_with_distinct
from src.db_engines.constants import MONGODB_FIND_MANY_MAX_COUNT
from constants_tests import DB_MONGO_CONFIG, DATABASES_MONGODB, COLLECTIONS_MONGODB

from ytpa_utils.val_utils import is_subset




""" Helper methods """
def reset_mongodb(engine: MongoDBEngine):
    for database in DATABASES_MONGODB.values():
        assert 'test' in database
        engine.delete_all_records_in_database(database)

def make_data() -> Dict[str, Dict[str, List[dict]]]:
    # make unique records for all databases and collections
    i = 0
    data = {}

    for database in DATABASES_MONGODB.values():
        data[database] = {}
        for collection in COLLECTIONS_MONGODB[database].values():
            data[database][collection] = [
                {
                    'text': database + collection + str(i) + str(j),
                    'number': int(str(i) + str(j)),
                    'text_nonunique': str(j // 100)
                }
                for j in range(MONGODB_FIND_MANY_MAX_COUNT + 100)
            ]
            i += 1

    return data

def setup_db_and_insert_records() -> Tuple[MongoDBEngine, List[dict]]:
    engine = MongoDBEngine(DB_MONGO_CONFIG, verbose=True)
    reset_mongodb(engine)

    database = list(DATABASES_MONGODB.values())[0]
    collection = list(COLLECTIONS_MONGODB[database].values())[0]

    data = make_data()
    data_ = data[database][collection]

    # insert records
    engine.set_db_info(database=database, collection=collection)
    engine.insert_many(data_)

    return engine, data_

def df_matches_with_dict(df: pd.DataFrame,
                         d: List[dict]) \
        -> bool:
    return df.equals(pd.DataFrame(d)[df.columns])




""" Tests """
def test_mongodb_engine_setup():
    engine = MongoDBEngine(DB_MONGO_CONFIG, verbose=True)
    assert DB_MONGO_CONFIG == engine.get_db_config()

    database = DATABASES_MONGODB['test1']
    collection = COLLECTIONS_MONGODB[database]['test11']

    # get_db_info()
    engine = MongoDBEngine(DB_MONGO_CONFIG, database=database, verbose=True)
    assert engine.get_db_info()[0] == database

    engine = MongoDBEngine(DB_MONGO_CONFIG, database=database, collection=collection, verbose=True)
    assert engine.get_db_info() == (database, collection)

    engine.set_db_info('1', '2')
    assert engine.get_db_info() == ('1', '2')

def test_creation_and_insert_one_and_find_one_ops():
    engine = MongoDBEngine(DB_MONGO_CONFIG, verbose=True)
    reset_mongodb(engine)

    # insert records (implicitly creates databases and collections)
    data = make_data()
    for database in DATABASES_MONGODB.values():
        for collection in COLLECTIONS_MONGODB[database].values():
            engine.set_db_info(database=database, collection=collection)
            engine.insert_one(data[database][collection][0])

    # check database and collection lists
    assert is_subset(DATABASES_MONGODB.values(), engine.get_all_databases())
    for database in DATABASES_MONGODB.values():
        collections_res = engine.get_all_collections(database=database)[database]
        collections_exp = COLLECTIONS_MONGODB[database].values()
        assert set(collections_res) == set(collections_exp)

    # check records
    for database in DATABASES_MONGODB.values():
        for collection in COLLECTIONS_MONGODB[database].values():
            engine.set_db_info(database=database, collection=collection)
            rec_exp = data[database][collection][0]
            rec_res = engine.find_one_by_id(rec_exp['_id'])
            assert rec_res == rec_exp

def test_find_many_gen():
    engine, data = setup_db_and_insert_records()

    # no options
    df = pd.concat([df_ for df_ in engine.find_many_gen()], ignore_index=True)
    assert df_matches_with_dict(df, data)

    # only filter
    filter = {'number': {'$gt': 50}}
    df = pd.concat([df for df in engine.find_many_gen(filter=filter)], ignore_index=True)
    d_exp = [d_ for d_ in data if d_['number'] > 50]
    assert df_matches_with_dict(df, d_exp)

    # only projection
    projection = {'_id': 0, 'text': 1}
    df = pd.concat([df for df in engine.find_many_gen(projection=projection)], ignore_index=True)
    d_exp = [{key: d_[key] for key in ['text']} for d_ in data]
    assert df_matches_with_dict(df, d_exp)

    # filter and projection
    filter = {'number': {'$gt': 50}}
    projection = {'_id': 0, 'text': 1}
    df = pd.concat([df for df in engine.find_many_gen(filter=filter, projection=projection)], ignore_index=True)
    d_exp = [{key: d_[key] for key in ['text']} for d_ in data if d_['number'] > 50]
    assert df_matches_with_dict(df, d_exp)

def test_find_one():
    engine, data = setup_db_and_insert_records()

    # no options
    rec = engine.find_one()
    assert any([rec == d_ for d_ in data])

    # only filter
    filter = {'number': {'$gt': 50}}
    rec = engine.find_one(filter=filter)
    assert any([rec == d_ for d_ in data if d_['number'] > 50])

    # only projection
    projection = {'_id': 0, 'text': 1}
    rec = engine.find_one(projection=projection)
    assert any([rec == {key: d_[key] for key in ['text']} for d_ in data])

    # filter and projection
    filter = {'number': {'$gt': 50}}
    projection = {'_id': 0, 'text': 1}
    rec = engine.find_one(filter=filter, projection=projection)
    assert any([rec == {key: d_[key] for key in ['text']} for d_ in data if d_['number'] > 50])

def test_find_many_by_ids():
    engine, data = setup_db_and_insert_records()

    data_exp = data[100:105]
    ids = [d_['_id'] for d_ in data_exp]

    # only ids
    recs = engine.find_many_by_ids(ids)
    assert all([rec in data_exp for rec in recs])

    # ids and limit
    recs = engine.find_many_by_ids(ids, limit=2)
    assert len(recs) == 2 and all([rec in data_exp for rec in recs])

    # ids and other filter
    filter = {'number': {'$in': [d_['number'] for d_ in data_exp[:2]]}}
    recs = engine.find_many_by_ids(ids, filter_other=filter)
    assert all([rec in data_exp[:2] for rec in recs])

def test_find_with_group_gen():
    engine, data = setup_db_and_insert_records()

    field = 'text_nonunique'

    # only field
    df = pd.concat([df_ for df_ in engine.find_distinct_gen(field)])
    assert set(df[field]) == set([d_['text_nonunique'] for d_ in data])

    # field and filter
    cols = ['1', '2']
    filter = {'$match': {'text_nonunique': {'$in': cols}}}
    df = pd.concat([df_ for df_ in engine.find_distinct_gen(field, filter=filter)])
    assert set(df[field]) == set([d_['text_nonunique'] for d_ in data if d_['text_nonunique'] in cols])

def test_delete_many():
    engine, data = setup_db_and_insert_records()

    data_modified = data[500:520]
    ids_to_delete = [d_['_id'] for d_ in data_modified]
    ids_to_keep = list(set([d_['_id'] for d_ in data]) - set(ids_to_delete))

    # delete some
    engine.delete_many(ids_to_delete)
    df = pd.concat([df_ for df_ in engine.find_many_gen()], ignore_index=True)
    assert set(ids_to_keep) == set(df['_id'])

    # delete all
    engine.delete_many({})
    ids_ = engine.get_ids()
    assert len(ids_) == 0



""" Utils """
def test_mongodb_utils():
    engine, data = setup_db_and_insert_records()

    database, collection = engine.get_db_info()

    """ get_mongodb_records_gen() """
    # only filter
    filter = {'number': [2, 3, 4]}
    df_gen = get_mongodb_records_gen(database, collection, DB_MONGO_CONFIG, filter=filter)
    df = pd.concat([df_ for df_ in df_gen], ignore_index=True)
    d_exp = [d_ for d_ in data if d_['number'] in [2, 3, 4]]
    assert df_matches_with_dict(df, d_exp)

    filter = {'number': [2, 3, 4], 'text_nonunique': '0'}
    df_gen = get_mongodb_records_gen(database, collection, DB_MONGO_CONFIG, filter=filter)
    df = pd.concat([df_ for df_ in df_gen], ignore_index=True)
    d_exp = [d_ for d_ in data if d_['number'] in [2, 3, 4] and d_['text_nonunique'] == '0']
    assert df_matches_with_dict(df, d_exp)

    filter = {'number': [[2, 4]]}
    df_gen = get_mongodb_records_gen(database, collection, DB_MONGO_CONFIG, filter=filter)
    df = pd.concat([df_ for df_ in df_gen], ignore_index=True)
    d_exp = [d_ for d_ in data if 2 <= d_['number'] <= 4]
    assert df_matches_with_dict(df, d_exp)

    # only projection
    projection = {'_id': 0, 'text': 1}
    df_gen = get_mongodb_records_gen(database, collection, DB_MONGO_CONFIG, projection=projection)
    df = pd.concat([df for df in df_gen], ignore_index=True)
    d_exp = [{key: d_[key] for key in ['text']} for d_ in data]
    assert df_matches_with_dict(df, d_exp)

    # filter and projection
    filter = {'number': {'$gt': 50}}
    projection = {'_id': 0, 'text': 1}
    df_gen = get_mongodb_records_gen(database, collection, DB_MONGO_CONFIG, filter=filter, projection=projection)
    df = pd.concat([df for df in df_gen], ignore_index=True)
    d_exp = [{key: d_[key] for key in ['text']} for d_ in data if d_['number'] > 50]
    assert df_matches_with_dict(df, d_exp)

    # distinct
    field = 'text_nonunique'
    cols = ['1', '2']
    filter = {'$match': {'text_nonunique': {'$in': cols}}}
    distinct = dict(group=field, filter=filter)  # filter is applied first
    df_gen = get_mongodb_records_gen(database, collection, DB_MONGO_CONFIG, distinct=distinct)
    df = pd.concat([df_ for df_ in df_gen], ignore_index=True)
    assert set(df[field]) == set([d_['text_nonunique'] for d_ in data if d_['text_nonunique'] in cols])

    """ load_all_recs_with_distinct() """
    # load all recs with distinct
    df = load_all_recs_with_distinct(database, collection, DB_MONGO_CONFIG, field, filter=filter)
    assert set(df[field]) == set([d_['text_nonunique'] for d_ in data if d_['text_nonunique'] in cols])






