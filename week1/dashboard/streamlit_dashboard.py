# pylint: disable = W0612
"""Creates a streamlit dashboard to show food truck data visualisations"""
from os import environ
import streamlit as st
import awswrangler as wr
import pandas as pd
from streamlit.delta_generator import DeltaGenerator
from dotenv import load_dotenv

# .env variables loaded so awswrangler can access them
load_dotenv()
AWS_SECRET_ACCESS_KEY = environ['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY_ID = environ['AWS_ACCESS_KEY_ID']
AWS_REGION = environ['AWS_REGION']
S3_BUCKET_NAME = environ['S3_BUCKET_NAME']
DATABASE_NAME = environ['DATABASE_NAME']

@st.cache_data
def get_truck_data() -> pd.DataFrame:
    """Returns a joined dataframe of all truck data"""
    sql_query = """
        SELECT
            transaction.transaction_id,
            transaction.total,
            transaction.at,
            pm.payment_method,
            truck.truck_name,
            truck.truck_description,
            truck.has_card_reader,
            truck.fsa_rating
        FROM transaction
        JOIN payment_method AS pm
            ON transaction.payment_method_id = pm.payment_method_id
        JOIN truck
            ON transaction.truck_id = truck.truck_id;
    """
    return wr.athena.read_sql_query(sql_query, DATABASE_NAME)


def get_total_over_time(df: pd.DataFrame, time_scale: str,
                              truck_filter: list) -> DeltaGenerator:
    """Generates a chart to show total revenue over time"""
    if time_scale == 'Hour':
        df['time_period'] = df['at'].dt.hour
    elif time_scale == 'Day':
        df['time_period'] = df['at'].dt.day_name()

    df['total'] = df['total'] / 100 # convert pence to pounds

    total_per_truck = df[df['truck_name'].isin(truck_filter)]
    total_per_truck = total_per_truck.groupby(
        ['time_period', 'truck_name'])['total'].sum().reset_index(name='total_revenue')

    st.subheader(f'Total Revenue Per {time_scale}')
    chart = st.bar_chart(
        total_per_truck,
        x = 'time_period',
        y = 'total_revenue',
        color = 'truck_name',
        x_label = f'{time_scale}',
        y_label = 'Total Revenue (£)'
        )
    return chart


def get_average_value_over_time(df: pd.DataFrame, time_scale: str,
                                truck_filter: list) -> DeltaGenerator:
    """Generates a chart to show average transaction value over time"""
    if time_scale == 'Hour':
        df['time_period'] = df['at'].dt.hour
    elif time_scale == 'Day':
        df['time_period'] = df['at'].dt.day_name()

    total_per_truck = df[df['truck_name'].isin(truck_filter)]
    total_per_truck = total_per_truck.groupby(
        ['time_period', 'truck_name'])['total'].mean().reset_index(
            name='average_transaction_value')

    st.subheader(f'Average Transaction Value Per {time_scale}')
    chart = st.bar_chart(
        total_per_truck,
        x = 'time_period',
        y = 'average_transaction_value',
        color = 'truck_name',
        x_label = f'{time_scale}',
        y_label = 'Average Transaction Value (£)'
        )
    return chart


def get_revenue_per_payment_method(df: pd.DataFrame, truck_filter: list) -> DeltaGenerator:
    """Generates a chart to show total revenue per payment method"""
    df['total'] = df['total'] / 100 # convert pence to pounds

    total_per_truck = df[df['truck_name'].isin(truck_filter)]
    total_per_truck = total_per_truck.groupby(
        ['payment_method', 'truck_name'])['total'].sum().reset_index(
            name='total_revenue')

    st.subheader('Total Revenue Per Payment Method')
    chart = st.bar_chart(
        total_per_truck,
        x = 'payment_method',
        y = 'total_revenue',
        color = 'truck_name',
        x_label = 'Payment Method',
        y_label = 'Total Revenue (£)'
        )
    return chart


if __name__ == '__main__':
    truck_df = get_truck_data()
    st.title('T3 Transaction Dashboard')
    with st.sidebar:
        trucks = list(truck_df['truck_name'].unique())
        truck_filter_selection = st.multiselect('Select Truck', trucks, default = trucks)
        time_scale_selection = st.radio(
            'Select Time Scale',
            ['Hour', 'Day']
        )
    get_total_over_time(truck_df, time_scale_selection, truck_filter_selection)
    get_average_value_over_time(truck_df, time_scale_selection, truck_filter_selection)
    get_revenue_per_payment_method(truck_df, truck_filter_selection)
