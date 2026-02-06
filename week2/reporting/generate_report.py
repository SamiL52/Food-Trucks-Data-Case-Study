# pylint: disable = W0613
"""Generates a daily T3 report of yesterdays truck transactions.
Configured to work as an AWS lambda function"""
from os import environ
from datetime import datetime
import awswrangler as wr
import pandas as pd
import boto3
from dotenv import load_dotenv

DATABASE_NAME = 'c20-sami-truck-database'

def query_highest_transaction_truck() -> pd.DataFrame:
    """Returns a dataframe of the food trucks sorted by total transactions"""
    load_dotenv()

    sql_query = """
        SELECT
            transaction.transaction_id,
            truck.truck_name,
            transaction.total,
            payment_method.payment_method,
            transaction.at
        FROM transaction
        JOIN truck
            ON truck.truck_id = transaction.truck_id
        JOIN payment_method
            ON payment_method.payment_method_id = transaction.payment_method_id
        WHERE CAST(year AS int) = year(date_add('day', -1, current_date))
        AND CAST(month AS int) = month(date_add('day', -1, current_date))
        AND CAST(day AS int) = day(date_add('day', -1, current_date));
    """

    session = boto3.Session(aws_access_key_id=environ["ACCESS_KEY_ID"],
                            aws_secret_access_key=environ["SECRET_ACCESS_KEY"],
                            region_name='eu-west-2')

    return wr.athena.read_sql_query(
        sql_query, DATABASE_NAME, boto3_session=session, ctas_approach=False).drop_duplicates()


def get_total_transaction_value(df: pd.DataFrame) -> float:
    """Returns the total value of all transactions in the given dataframe"""
    return df['total'].sum()


def get_total_transaction_number(df: pd.DataFrame) -> int:
    """Returns the number of transactions in the given dataframe"""
    return df.size


def get_transactions_per_truck(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the number of transactions per truck"""
    return (
        df.groupby('truck_name')
        .size()
        .reset_index(name='transaction_count')
        .sort_values(by='transaction_count', ascending=False)
    )


def get_transaction_value_per_truck(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of the total revenue per truck"""
    return (
        df.groupby('truck_name')['total']
        .sum()
        .reset_index(name='total_value')
        .sort_values(by='total_value', ascending=False)
    )


def generate_daily_report(all_data: pd.DataFrame) -> None:
    """Queries the truck database to generate a daily report"""
    today = datetime.today().date()
    total_revenue = get_total_transaction_value(all_data)
    number_of_sales = get_total_transaction_number(all_data)
    number_of_sales_per_truck = get_transactions_per_truck(all_data)
    revenue_per_truck = get_transaction_value_per_truck(all_data)

    report_data = {
        "date": today,
        "total_revenue": total_revenue,
        "number_of_sales": number_of_sales,
        "number_of_sales_per_truck": number_of_sales_per_truck, #.to_dict(orient='records'),
        "revenue_per_truck": revenue_per_truck #.to_dict(orient='records')
    }

    return report_data


def generate_html_text(report_data: dict) -> str:
    """Generates a formatted html report for the given data"""
    html_text = f"""
        <head>
            <title>T3 DAILY REPORT</title>
        </head>
        <style>
            table {{
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 20px;
            }}
            th, td {{
                border: 1px solid #999;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
        <body style="font-family: Arial;">
            <center>
            <h1>T3 DAILY REPORT</h1>
            <p>{report_data.get('date')}</p>

            <h2>Key Metrics</h2>

            <li>Total Transactions: {report_data.get('number_of_sales')}</li>
            <li>Total Revenue: £{report_data.get('total_revenue')/100}</li>

            <h2>Trucks</h2>
            <h3>Number Of Sales Per Truck</h3>
            {report_data.get('number_of_sales_per_truck').to_html(index=False)}
            <h3>Total Revenue Per Truck</h3>
            {report_data.get('revenue_per_truck').to_html(index=False)}
            </center>
        </body>
    """

    return html_text


def save_html_report_to_file(html_text: str) -> None:
    """Saves the given html string to an html file with today's date as a name"""
    with open(f'report_data_{datetime.today().date()}.html', 'w', encoding='utf-8') as f:
        f.write(html_text)


def lambda_handler(event: dict, context: dict) -> dict[str, str]:
    """The entry point for the AWS Lambda"""
    data = query_highest_transaction_truck()
    formatted_data = generate_daily_report(data)
    html_text = generate_html_text(formatted_data)

    return {"html": html_text}


if __name__ == '__main__':
    truck_data = query_highest_transaction_truck()
    print(truck_data)
    formatted_truck_data = generate_daily_report(truck_data)
    output_html = generate_html_text(formatted_truck_data)
    save_html_report_to_file(output_html)
