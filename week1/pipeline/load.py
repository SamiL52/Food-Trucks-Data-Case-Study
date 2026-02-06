# pylint: disable = W0612
# pylint: disable = C0103
import pandas as pd
import awswrangler as wr
from os import environ
from dotenv import load_dotenv

# .env variables loaded so awswrangler can access them
load_dotenv()
AWS_SECRET_ACCESS_KEY = environ['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY_ID = environ['AWS_ACCESS_KEY_ID']
AWS_REGION = environ['AWS_REGION']
S3_BUCKET_NAME = environ['S3_BUCKET_NAME']
S3_FILEPATH = 's3://c20-sami-truck-s3-bucket/input/'


def save_data_to_parquet() -> dict[pd.DataFrame]:
    """Loads and saves truck and payment data to parquet files"""
    truck_df = pd.read_csv('data/clean_truck.csv')
    payment_df = pd.read_csv('data/clean_payment_method.csv')

    truck_df.to_parquet('data/clean_truck.parquet')
    payment_df.to_parquet('data/clean_payment_method.parquet')

    upload_parquet_to_s3(truck_df, 'truck', False)
    upload_parquet_to_s3(payment_df, 'payment_method', False)

def create_time_partitioned_parquet() -> None:
    """Loads and saves transaction data to time partitioned parquet files"""
    transaction_df = pd.read_csv('data/clean_transaction.csv')
    transaction_df['at'] = pd.to_datetime(transaction_df['at'])

    transaction_df['year'] = transaction_df['at'].dt.year
    transaction_df['month'] = transaction_df['at'].dt.month
    transaction_df['day'] = transaction_df['at'].dt.day
    transaction_df['hour'] = transaction_df['at'].dt.hour

    transaction_df.to_parquet('data/clean_transaction.parquet', partition_cols=['year', 'month', 'day', 'hour'])

    upload_parquet_to_s3(transaction_df, 'transaction', True)


def upload_parquet_to_s3(data: pd.DataFrame, filename: str, is_time_partitioned: bool = False) -> None:
    """Uploads parquet files to the S3 bucket. Has a flag to denote time partition."""
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
    save_data_to_parquet()
    create_time_partitioned_parquet()
