"""Run tests in a loop. Helpful for debugging speed and memory"""

import time
import tracemalloc

import numpy as np

from tests.test_mysql import (test_engine_setup, test_describe_table, test_select_records,
                              test_select_records_with_join, test_get_table_colnames, test_get_table_primary_keys,
                              test_insert_and_update_records_from_dict)

from tests.test_mongodb import (test_mongodb_engine_setup, test_creation_and_insert_one_and_find_one_ops,
                                test_find_many_gen, test_find_one, test_find_many_by_ids, test_find_with_group_gen,
                                test_delete_many, test_mongodb_utils, reset_mongodb)

from typing import Dict, List, Tuple

from src.db_engines.mongodb_engine import MongoDBEngine
from tests.constants_tests import DB_MONGO_CONFIG, DATABASES_MONGODB, COLLECTIONS_MONGODB




def run_mysql_tests(dur: float):
    t_start = time.time()

    tracemalloc.start()
    snapshots = [tracemalloc.get_traced_memory()]
    i = 0

    while time.time() - t_start < dur:
        i += 1

        print(f"Iter {i}")

        test_engine_setup()
        test_describe_table()
        test_select_records()
        test_select_records_with_join()
        test_get_table_colnames()
        test_get_table_primary_keys()
        test_insert_and_update_records_from_dict()

        snapshots.append(tracemalloc.get_traced_memory())

    print('\n\n')
    for i, (size_, peak_) in enumerate(snapshots):
        print(f"Iter {i}: {size_=}, {peak_=}")

    tracemalloc.stop()


def make_data_debug() -> Dict[str, Dict[str, List[dict]]]:
    # make unique records for all databases and collections
    i = 0
    data = {}

    for database in DATABASES_MONGODB.values():
        data[database] = {}
        for collection in COLLECTIONS_MONGODB[database].values():
            data[database][collection] = [
                {
                    'var0': np.random.randn(1000).tolist(),
                    'var1': 'asodfyao87' * 100,
                    'var2': ['asodfyao87'] * 100
                }
                for _ in range(10000)
            ]
            i += 1

    return data

def setup_db_and_insert_records_debug(data) -> Tuple[MongoDBEngine, List[dict]]:
    engine = MongoDBEngine(DB_MONGO_CONFIG, verbose=True)
    reset_mongodb(engine)

    database = list(DATABASES_MONGODB.values())[0]
    collection = list(COLLECTIONS_MONGODB[database].values())[0]

    data_ = data[database][collection]

    # insert records
    engine.set_db_info(database=database, collection=collection)
    engine.insert_many(data_)

    return engine, data_

def run_mongodb_tests(dur: float):
    t_start = time.time()

    tracemalloc.start()
    snapshots = [tracemalloc.get_traced_memory()]
    i = 0

    data = make_data_debug()

    while time.time() - t_start < dur:
        i += 1

        print(f"Iter {i}")

        engine, _ = setup_db_and_insert_records_debug(data)
        # df_gen = engine.find_many_gen()
        # for _ in df_gen:
        #     pass

        # test_mongodb_engine_setup()
        # test_creation_and_insert_one_and_find_one_ops()
        # test_find_many_gen()
        # test_find_one()
        # test_find_many_by_ids()
        # test_find_with_group_gen()
        # test_delete_many()
        # test_mongodb_utils()

        reset_mongodb(engine)

        snapshots.append(tracemalloc.get_traced_memory())

    print('\n\n')
    for i, (size_, peak_) in enumerate(snapshots):
        print(f"Iter {i}: size={size_/1e6} MB, peak={peak_/1e6} MB")

    tracemalloc.stop()




if __name__ == '__main__':
    dur = 60 * 30
    # run_mysql_tests(dur)
    run_mongodb_tests(dur)

