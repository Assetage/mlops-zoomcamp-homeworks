#!/usr/bin/env python
# coding: utf-8

import sys
import os
import pickle
import pandas as pd

options = {
    'client_kwargs': {
        'endpoint_url': "http://localhost:4566"
    }
}


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def read_data(filename):
    return pd.read_parquet(filename, storage_options=options)

def save_data(df, output_file):
    df.to_parquet(output_file, engine='pyarrow', index=False, storage_options=options)
    

def run(year, month):

    os.environ['INPUT_FILE_PATTERN'] = "s3://nyc-duration/{year:04d}-{month:02d}.parquet"
    os.environ['OUTPUT_FILE_PATTERN'] = "s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"

    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    df = read_data(input_file)
    
    categorical = ['PULocationID', 'DOLocationID']
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())
    print('predicted sum duration:', y_pred.sum())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    print("input file", input_file)
    print("output file", output_file)

    save_data(df = df_result, 
              output_file = output_file)

year = int(sys.argv[1])
month = int(sys.argv[2])
run(year, month)