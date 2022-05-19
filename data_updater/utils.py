import requests
import json
import math
import pandas as pd

from typing import List, Tuple
from transactions import Transaction
from requests.structures import CaseInsensitiveDict

# Method used to fetch all the transactions from chosen start_date
# We retrieve the total_count in the first call to assess the number of pages remaining given the "max_txs" parameter
def fetch_all_transactions(
    token: str, URL: str, start_date: str, max_txs: int = 1000
) -> List[Transaction]:

    (total_count, transactions) = fetch_transaction(token, URL, start_date, max_txs)

    pages_to_fetch = math.ceil(total_count / max_txs)

    if total_count > max_txs:
        for page in range(1, pages_to_fetch):
            (_, transactions_to_add) = fetch_transaction(token, URL, start_date, max_txs, page=page)
            transactions += transactions_to_add
    return transactions


# Basic call to fetch one page of data


def fetch_transaction(
    token: str, URL: str, start_date: str, max_txs: int, page: int = 0
) -> Tuple[int, List[Transaction]]:

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Bearer {token}"

    params = {"start_date": start_date, "max_txs": max_txs, "page": page}

    response = requests.get(URL, params=params, headers=headers)
    if response.status_code == 200:
        response_json = response.json()

        total_count = response_json["total_count"]
        transactions = [
            Transaction(**transaction) for transaction in json.loads(response_json["data"])
        ]
        return (total_count, transactions)

    response.raise_for_status()


# If the targeted file path already exists, we only fetch and add the incremental data


def ingest_incremental_data(token: str, URL: str, csv_path: str) -> int:
    df_trx = pd.read_csv(csv_path)
    (last_transaction_epoch, last_transaction_date) = (
        # We retrieve the last date and last datetime fetched to add only the incremental data
        df_trx.iloc[df_trx["transaction_datetime_epoch"].idxmax()]["transaction_datetime_epoch"],
        df_trx.iloc[df_trx["transaction_datetime_epoch"].idxmax()]["transaction_date"],
    )

    trx_to_ingest = fetch_all_transactions(token, URL, start_date=last_transaction_date)

    df_trx_from_start_date = pd.DataFrame([s.__dict__ for s in trx_to_ingest])
    df_trx_from_datetime = df_trx_from_start_date[
        df_trx_from_start_date["transaction_datetime_epoch"] > last_transaction_epoch
    ]

    df_trx_updated = pd.concat([df_trx, df_trx_from_datetime], ignore_index=True, sort=False)
    rows_to_write = len(df_trx_from_datetime)
    df_trx_updated.to_csv(csv_path, index=False)

    return rows_to_write


# If the targeted file path does not exists, we fetch all the data since chosen start date

def ingest_complete_data(token: str, URL: str, csv_path: str, start_date: str) -> int:
    trx_to_ingest = fetch_all_transactions(token, URL, start_date=start_date)

    df_all_trx = pd.DataFrame([s.__dict__ for s in trx_to_ingest])
    rows_to_write = len(df_all_trx)
    df_all_trx.to_csv(csv_path, index=False)

    return rows_to_write
