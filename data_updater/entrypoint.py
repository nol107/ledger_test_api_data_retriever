import configparser
import sys

from utils import (
    ingest_complete_data,
    ingest_incremental_data,
)
from os import path


config_api = configparser.ConfigParser()
config_api.read("../config_api.ini")

token = config_api["Transaction_API"]["token"]
default_url = config_api["Transaction_API"]["default_url"]
default_start_date = config_api["Transaction_API"]["default_start_date"]
default_path = config_api["Transaction_API"]["default_path"]


def main(
    token: str = token,
    url: str = default_url,
    start_date: str = default_start_date,
    file_path: str = default_path,
):
    if path.exists(file_path):
        written_rows = ingest_incremental_data(token, url, file_path)
    else:
        written_rows = ingest_complete_data(token, url, file_path, start_date)
    print("-------------------------------------------------------------------------------------")
    print(f"SUCCESSFULLY WROTE {written_rows} rows in {file_path}")
    print("-------------------------------------------------------------------------------------")


# Only one argument on the main function (to change the path)
# We could think of 2 other arguments : "--reset" to rewrite a file from start date & "--completion" to also ingest the missing data from the start_date (not only new transactions)
if __name__ == "__main__":

    if len(sys.argv) > 2 and sys.argv[1] == "--path":
        main(file_path=sys.argv[2])
    else:
        main()
