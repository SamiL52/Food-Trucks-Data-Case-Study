"""Extracts food truck data from RDS"""
from os import environ
from datetime import datetime
from pymysql import connect, Connection
from dotenv import load_dotenv
import pandas as pd
import awswrangler as wr

DATABASE_NAME = 'c20-sami-truck-database'

def get_db_connection() -> None:
    """Returns a live connection to the database"""
    load_dotenv()
    conn = connect(
        host = environ['DB_HOST'],
        user = environ['DB_USER'],
        password = environ['DB_PASSWORD'],
        database = environ['DB_NAME'],
        port = 3306
    )

    return conn


def get_most_recent_timestamp() -> datetime | None:
    """Returns a datetime for the most recent entry found in the database"""
    sql_query = """
        SELECT
            at
        FROM transaction
        ORDER BY at DESC
        LIMIT 1;
    """

    try:
        timestamp = wr.athena.read_sql_query(sql_query, DATABASE_NAME)['at'][0]
        return timestamp.to_pydatetime()
    except wr.exceptions.QueryFailed as e:
        print(f'Could not retrieve most recent timestamp from database: {e}')
        return None


def get_all_data() -> pd.DataFrame:
    """Debug function to get all data from the database with athena"""
    sql_query = """
        SELECT *
        FROM transaction
        ORDER BY at DESC;
    """

    data = wr.athena.read_sql_query(sql_query, DATABASE_NAME)
    return data


def download_save_data(conn: Connection, timestamp: datetime) -> None:
    """Downloads and saves all data from the given database connection"""
    truck_df = pd.read_sql('SELECT * FROM DIM_Truck;', conn)
    payment_method_df = pd.read_sql('SELECT * FROM DIM_Payment_Method;', conn)
    transaction_df = pd.read_sql(f"""
            SELECT * FROM FACT_Transaction
            WHERE at >= '{timestamp}'
            ORDER BY at DESC;
        """, conn)

    truck_df.to_csv('./data/truck.csv', index = False)
    payment_method_df.to_csv('./data/payment_method.csv', index = False)
    transaction_df.to_csv('./data/transaction.csv', index = False)

if __name__ == '__main__':
    most_recent_timestamp = get_most_recent_timestamp()
    connection = get_db_connection()
    download_save_data(connection, most_recent_timestamp)
    connection.close()
    # print(get_all_data())
