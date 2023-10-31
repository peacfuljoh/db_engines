"""Utils that use the MongoDB engine."""

from typing import Optional, Generator

import pandas as pd
from ytpa_utils.val_utils import is_list_of_strings

from .mongodb_engine import MongoDBEngine


def get_mongodb_records_gen(database: str,
                            collection: str,
                            db_config: dict,
                            filter: Optional[dict] = None,
                            projection: Optional[dict] = None,
                            distinct: Optional[dict] = None) \
        -> Generator[pd.DataFrame, None, None]:
    """
    Prepare MongoDB feature generator with extract configuration.
    Provide (filter and/or projection) or distinct, or neither, but not both. Specifying both will raise an error.
    """
    using_filt = filter is not None
    using_proj = projection is not None
    using_dist = distinct is not None
    using_filt_or_proj = using_filt or using_proj
    assert not (using_filt_or_proj and using_dist) # using one or the other, but not both

    engine = MongoDBEngine(db_config, database=database, collection=collection)
    if distinct is not None:
        assert 'group' in distinct
        return engine.find_distinct_gen(distinct['group'], filter=distinct.get('filter'))
    else:
        filter_for_req: dict = {}
        if filter:
            for key, val in filter.items():
                if isinstance(val, str):
                    filter_for_req[key] = val
                elif is_list_of_strings(val):
                    filter_for_req[key] = {'$in': val}
                else:
                    raise NotImplementedError(f"Filter type not yet implemented: {key}: {val}.")

        return engine.find_many_gen(filter_for_req, projection=projection)


def load_all_recs_with_distinct(database: str,
                                collection: str,
                                db_config: dict,
                                group: str,
                                filter: Optional[dict] = None) \
        -> pd.DataFrame:
    """Load many distinct records (optional filter followed by distinct query)."""
    distinct_ = dict(group=group, filter=filter)  # filter is applied first
    df_gen = get_mongodb_records_gen(database, collection, db_config, distinct=distinct_)
    return pd.concat([df for df in df_gen], axis=0, ignore_index=True)
