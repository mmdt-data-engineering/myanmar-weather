import pandas as pd
from db_utils import PostgresDB

import os
import pandas as pd


def get_csv_files(folder_path: str) -> list:
    """
    Get a list of CSV files in the specified folder.
    """
    # Check if the folder exists
    if not os.path.exists(folder_path):
        raise FileNotFoundError(
            f"Folder not found: {folder_path} (working dir: {os.getcwd()})"
        )

    # List all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    return csv_files


def load_all_files_to_localdb():
    """Load CSV files from a folder to local PostgreSQL database."""

    folder_path = "./output"
    csv_files = get_csv_files(folder_path)

    for file in csv_files:

        df = pd.read_csv(os.path.join(folder_path, file))
        df.Name = os.path.splitext(file)[0]

        print(f"DataFrame Name: {df.Name}")
        print(f"DataFrame Shape: {df.shape}")

        db = PostgresDB()

        # Load the DataFrame to local PostgreSQL
        db.load_to_postgres(df, table_name=df.Name)


def load_file_to_db(file_path: str):
    """
    Load CSV file to Neon PostgreSQL database.

    Parameters:
    file_path (str): Path to the CSV file to be loaded.

    """

    df = pd.read_csv(file_path)

    filename = os.path.basename(file_path)
    # date_part = os.path.splitext(filename)[0].split("_")[0]
    # df.Name = filename.replace(f"{date_part}_", "").replace(".csv", "")
    df.Name = os.path.splitext(filename)[0]

    print(f"DataFrame Name: {df.Name}")
    print(f"DataFrame Shape: {df.shape}")

    db = PostgresDB()

    # Load the DataFrame to Neon PostgreSQL
    db.load_to_neon_postgres(df, table_name=df.Name)
