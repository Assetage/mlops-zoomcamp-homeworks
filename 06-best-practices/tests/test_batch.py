from datetime import datetime
import pandas as pd
from batch import prepare_data

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df = pd.DataFrame(data, columns=columns)

def test_prepare_data():
    actual_df = prepare_data(df, columns)
    expected_df = pd.DataFrame({'PULocationID': {0: '-1', 1: '1'}, 
                                'DOLocationID': {0: '-1', 1: '1'}, 
                                'tpep_pickup_datetime': {0: '1672534860000000000', 1: '1672534920000000000'}, 
                                'tpep_dropoff_datetime': {0: '1672535400000000000', 1: '1672535400000000000'}, 
                                'duration': {0: 9.0, 1: 8.0}})
    # number of rows in the expected dataframe - 2
    pd.testing.assert_frame_equal(actual_df, expected_df)