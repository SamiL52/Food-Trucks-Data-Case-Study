# pylint: disable = W0612
"""The data validation and cleaning step of the pipeline"""
import pandas as pd


def load_all_data() -> pd.DataFrame:
    """Returns 3 dataframes of all the data for payment_method, transaction and truck"""
    payment_method_df = pd.read_csv('data/payment_method.csv')
    transaction_df = pd.read_csv('data/transaction.csv')
    truck_df = pd.read_csv('data/truck.csv')

    return {
        "transaction": transaction_df,
        "payment_method": payment_method_df,
        "truck": truck_df
    }


def save_all_data(all_data: dict[str: pd.DataFrame]) -> None:
    """Saves keys in the given dict to a 'clean' csv file"""
    for key in all_data:
        all_data[key].to_csv(f'./data/clean_{key}.csv', index = False)


def clean_transaction_data(transaction_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the data in the transaction table"""
    transaction_df['transaction_id'] = (
        pd.to_numeric(transaction_df['transaction_id'], errors='coerce'))

    transaction_df['truck_id'] = pd.to_numeric(transaction_df['truck_id'], errors='coerce')
    transaction_df = transaction_df[transaction_df['truck_id'].between(1, 6)]

    transaction_df['payment_method_id'] = (
        pd.to_numeric(transaction_df['payment_method_id'], errors='coerce'))
    transaction_df = transaction_df[transaction_df['payment_method_id'].isin([1, 2])]

    transaction_df['total'] = pd.to_numeric(transaction_df['total'], errors='coerce')
    transaction_df = transaction_df[transaction_df['total'] != 0]

    transaction_df['at'] = pd.to_datetime(transaction_df['at'], errors='coerce')

    return transaction_df


def clean_payment_method_data(payment_method_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the data in the payment_method table"""
    payment_method_df['payment_method'] = payment_method_df['payment_method'].astype(str)

    return payment_method_df


def clean_truck_data(truck_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the data in the truck table"""
    truck_df['truck_name'] = truck_df['truck_name'].astype(str)
    truck_df = truck_df[truck_df['fsa_rating'].between(1, 5)]
    truck_df['truck_description'] = truck_df['truck_description'].astype(str)
    truck_df['has_card_reader'] = pd.to_numeric(truck_df['has_card_reader'], errors='coerce')

    return truck_df


def clean_all_data(all_data: dict[str: pd.DataFrame]) -> pd.DataFrame:
    """Iterates through all tables and cleans their data"""

    all_data['truck'] = clean_truck_data(all_data['truck'])
    all_data['payment_method'] = clean_payment_method_data(all_data['payment_method'])
    all_data['transaction'] = clean_transaction_data(all_data['transaction'])

    for key in all_data:
        all_data[key] = all_data[key].dropna()

    return all_data


if __name__ == '__main__':
    data = load_all_data()
    clean_data = clean_all_data(data)
    save_all_data(clean_data)
