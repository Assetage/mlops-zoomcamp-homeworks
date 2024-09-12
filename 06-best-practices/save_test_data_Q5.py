import pandas as pd
from batch_Q4 import options

year = int("2023")
month = int("01")
save_path = "s3://nyc-duration/{year:04d}-{month:02d}.parquet".format(year=year, month=month)

expected_df = pd.DataFrame({'PULocationID': {0: '-1', 1: '1'}, 
                                'DOLocationID': {0: '-1', 1: '1'}, 
                                'tpep_pickup_datetime': {0: '1672534860000000000', 1: '1672534920000000000'}, 
                                'tpep_dropoff_datetime': {0: '1672535400000000000', 1: '1672535400000000000'}, 
                                'duration': {0: 9.0, 1: 8.0}})

expected_df.to_parquet(
    save_path,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)