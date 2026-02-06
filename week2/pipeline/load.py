# pylint: disable = W0612
# pylint: disable = C0103
"""The data uploading step of the pipeline.
Pushes cleaned data as parquet files to an S3 bucket."""
from os import environ
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv

load_dotenv()
AWS_SECRET_ACCESS_KEY = environ['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY_ID = environ['AWS_ACCESS_KEY_ID']
AWS_REGION = environ['AWS_REGION']
S3_BUCKET_NAME = environ['S3_BUCKET_NAME']
S3_FILEPATH = 's3://c20-sami-truck-s3-bucket/input/'


def save_and_upload_parquet() -> dict[pd.DataFrame]:
    """Saves truck and payment data as parquet files, and uploads them to an S3"""
    truck_df = pd.read_csv('data/clean_truck.csv')
    payment_df = pd.read_csv('data/clean_payment_method.csv')

    truck_df.to_parquet('data/clean_truck.parquet')
    payment_df.to_parquet('data/clean_payment_method.parquet')

    upload_parquet_to_s3(truck_df, 'truck', False)
    upload_parquet_to_s3(payment_df, 'payment_method', False)


def save_and_upload_partitioned_parquet() -> None:
    """Saves transaction data as partitioned parquet files, and uploads them to an S3"""
    transaction_df = pd.read_csv('data/clean_transaction.csv')
    transaction_df['at'] = pd.to_datetime(transaction_df['at'])

    transaction_df['year'] = transaction_df['at'].dt.year
    transaction_df['month'] = transaction_df['at'].dt.month
    transaction_df['day'] = transaction_df['at'].dt.day
    transaction_df['hour'] = transaction_df['at'].dt.hour

    transaction_df.to_parquet('data/clean_transaction.parquet',
                              partition_cols=['year', 'month', 'day', 'hour'])

    upload_parquet_to_s3(transaction_df, 'transaction', True)


def upload_parquet_to_s3(
        data: pd.DataFrame, filename: str, is_time_partitioned: bool = False) -> None:
    """Uploads parquet files to an S3. Can handle both partitioned and non-partitioned"""
    if is_time_partitioned:
        wr.s3.to_parquet(
            df = data,
            path = f'{S3_FILEPATH}{filename}/{filename}.parquet',
            dataset = True,
            partition_cols = ['year', 'month', 'day', 'hour']
        )
        return

    wr.s3.to_parquet(
        df = data,
        path = f'{S3_FILEPATH}{filename}/{filename}.parquet',
        dataset = False
    )


if __name__ == '__main__':
    save_and_upload_parquet()
    save_and_upload_partitioned_parquet()
