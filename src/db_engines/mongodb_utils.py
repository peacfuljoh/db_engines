"""Utils that use the MongoDB engine."""

from typing import Optional, Generator, Tuple

import pandas as pd
from ytpa_utils.val_utils import is_list_of_instances

from .mongodb_engine import MongoDBEngine


def get_mongodb_records_gen(database: str,
                            collection: str,
                            db_config: dict,
                            filter: Optional[dict] = None,
                            projection: Optional[dict] = None,
                            distinct: Optional[dict] = None) \
        -> Generator[pd.DataFrame, None, None]:
    """
    Prepare MongoDB feature generator with some options. See find_distinct_gen() and find_many_gen() for the format of
    the provided options.

    Provide (filter and/or projection) or distinct, but not both. Ex:
        - provide filter
        - provide projection
        - provide filter and projection
        - provide distinct

    If using 'distinct' input arg, filter is specified through the distinct dict:
        e.g. distinct = dict(group=<str>, filter=<dict>)

    Filter options:
        - equality: filter = dict(a='5')
        - set membership: filter = dict(b=[1, 2, 3])
        - MongoDB-formatted: filter = {'$gt': 50}
    """
    using_filt = filter is not None
    using_proj = projection is not None
    using_dist = distinct is not None
    using_filt_or_proj = using_filt or using_proj
    assert not (using_filt_or_proj and using_dist) # using one or the other, but not both

    engine = MongoDBEngine(db_config, database=database, collection=collection)

    if using_dist:
        assert 'group' in distinct
        return engine.find_distinct_gen(distinct['group'], filter=distinct.get('filter'))
    elif using_filt_or_proj:
        filter_for_req: dict = {}
        if filter:
            for key, val in filter.items():
                if isinstance(val, str): # equality
                    filter_for_req[key] = val
                elif is_list_of_instances(val, (str, int)): # set membership
                    filter_for_req[key] = {'$in': val}
                elif isinstance(val, dict): # MongoDB-formatted
                    assert all(['$' in key_ for key_ in val])
                    filter_for_req[key] = val
                else:
                    raise NotImplementedError(f"Filter type not yet implemented: {key}: {val}.")

        return engine.find_many_gen(filter_for_req, projection=projection)
    else:
        raise NotImplementedError('You must provide at least one of the options (filter, projection, distinct).')


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
