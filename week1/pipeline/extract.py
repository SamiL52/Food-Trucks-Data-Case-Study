"""Extracts food truck data from RDS"""
from os import environ
from pymysql import connect, Connection
from dotenv import load_dotenv
import pandas as pd

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


def download_save_data(conn: Connection) -> None:
    """Downloads and saves all data from the given database connection"""
    truck_df = pd.read_sql('SELECT * FROM DIM_Truck;', conn)
    payment_method_df = pd.read_sql('SELECT * FROM DIM_Payment_Method;', conn)
    transaction_df = pd.read_sql('SELECT * FROM FACT_Transaction;', conn)

    truck_df.to_csv('./data/truck.csv', index = False)
    payment_method_df.to_csv('./data/payment_method.csv', index = False)
    transaction_df.to_csv('./data/transaction.csv', index = False)


if __name__ == '__main__':
    connection = get_db_connection()
    download_save_data(connection)
    connection.close()
