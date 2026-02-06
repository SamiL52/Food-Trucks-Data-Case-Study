import awswrangler as wr
import pandas as pd

DATABASE_NAME = 'c20-sami-truck-database'

def query_highest_transaction_truck() -> pd.DataFrame:
    """Returns a dataframe of the food trucks sorted by total transactions"""
    sql_query = """
        SELECT
            truck.truck_name,
            COUNT(*) AS count
        FROM transaction
        JOIN truck
            ON truck.truck_id = transaction.truck_id
        GROUP BY truck_name
        ORDER BY count DESC;
    """
    return wr.athena.read_sql_query(sql_query, DATABASE_NAME)

def query_lowest_value_truck() -> pd.DataFrame:
    """Returns a dataframe of the food trucks sorted by least total transaction value"""
    sql_query = """
        SELECT
            truck.truck_name,
            SUM(total) AS total_value
        FROM transaction
        JOIN truck
            ON truck.truck_id = transaction.truck_id
        GROUP BY truck_name
        ORDER BY total_value ASC;
    """
    return wr.athena.read_sql_query(sql_query, DATABASE_NAME)

def query_average_transaction_value() -> pd.DataFrame:
    """Returns the average transaction value across all transactions"""
    sql_query = """
        SELECT
            AVG(total) as average
        FROM transaction;
    """
    return wr.athena.read_sql_query(sql_query, DATABASE_NAME)['average'][0]

def query_average_transaction_value_per_truck() -> pd.DataFrame:
    """Returns the average transaction value per truck"""
    sql_query = """
        SELECT
            truck.truck_name,
            AVG(total) as average
        FROM transaction
        JOIN truck
            ON truck.truck_id = transaction.truck_id
        GROUP BY truck.truck_name
        ORDER BY average DESC;
    """
    return wr.athena.read_sql_query(sql_query, DATABASE_NAME)

def query_cash_proportion() -> pd.DataFrame:
    """Returns the proportion of transactions that use cash"""
    sql_query = """
        SELECT
            (COUNT(*) * 1.0) / (SELECT COUNT(*) FROM transaction) AS cash_proportion
        FROM transaction
        JOIN payment_method
            ON payment_method.payment_method_id = transaction.payment_method_id
        WHERE payment_method.payment_method = 'cash';
    """

    return wr.athena.read_sql_query(sql_query, DATABASE_NAME)


if __name__ == '__main__':
    print(query_highest_transaction_truck())
    print(query_lowest_value_truck())
    print(query_average_transaction_value())
    print(query_average_transaction_value_per_truck())
    print(query_cash_proportion())