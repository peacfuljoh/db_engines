
from src.db_engines.time_utils import get_ts_now_str



if __name__ == '__main__':
    if 1:
        print(get_ts_now_str(mode='date'))
        print(get_ts_now_str(mode='s'))
        print(get_ts_now_str(mode='ms'))
        print(get_ts_now_str(mode='us'))